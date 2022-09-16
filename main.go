package main

import (
	"context"
	"fmt"
	"log"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"math/rand"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	metrics "k8s.io/metrics/pkg/client/clientset/versioned"
)

// type Filter func(node *v1.Node, pod *v1.Pod) (string,bool)

type CustomKubeScheduler struct {
	ClientSet  *kubernetes.Clientset
	PodQueue   chan *v1.Pod
	NodeLister listers.NodeLister
	// FilterFunc []Filter
}

func initInformers(clientset *kubernetes.Clientset, podQueue chan *v1.Pod, quit chan struct{}) listers.NodeLister {
	factory := informers.NewSharedInformerFactory(clientset, 0)

	nodeInformer := factory.Core().V1().Nodes()
	nodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			node, ok := obj.(*v1.Node)
			if !ok {
				log.Println("this is not a node")
				return
			}
			log.Printf("New Node Added to Cluster: %s", node.GetName())
		},
	})

	podInformer := factory.Core().V1().Pods()
	podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod, ok := obj.(*v1.Pod)
			if !ok {
				log.Println("this is not a pod")
				return
			}
			if pod.Spec.NodeName == "" && pod.Spec.SchedulerName == "customScheduler" {
				podQueue <- pod
			}
		},
	})

	factory.Start(quit)
	return nodeInformer.Lister()
}

func NewScheduler(podQueue chan *v1.Pod, quit chan struct{}) CustomKubeScheduler {
	clientset := clientSetup()

	return CustomKubeScheduler{
		ClientSet:  clientset,
		PodQueue:   podQueue,
		NodeLister: initInformers(clientset, podQueue, quit),
		// FilterFunc: []Filter{podLabelIsThereInNode},
	}
}

func (ks *CustomKubeScheduler) schedulePod() {

	pod := <-ks.PodQueue
	fmt.Println("found a pod to schedule:", pod.Namespace, "/", pod.Name)

	node := ks.findFit(pod)

	if node != "nil" {
		err := ks.bindPod(pod, node)
		if err != nil {
			log.Println("failed to bind pod", err)
			return
		}
	}

	message := fmt.Sprintf("Scheduled pod [%s/%s] on %s with label %s\n", pod.Namespace, pod.Name, node, pod.Labels["app"])

	err := ks.emitEvent(pod, message)
	if err != nil {
		log.Println("failed to emit pod schedule event", err)
		return
	}

	fmt.Println(message)
}
func GetNodeCapacity(nodeName string) (int64, *resource.Quantity, bool, bool) {
	clientset := clientSetup()
	nodes, err := clientset.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})

	if err != nil {
		fmt.Println(err)
	}
	for _, v := range nodes.Items {

		if v.Name == nodeName {
			fmt.Println(len(v.Spec.Taints))
			for _, t := range v.Spec.Taints{
				if t.Value == "spot" && t.Effect == "NoSchedule" {
					return v.Status.Capacity.Cpu().MilliValue(), v.Status.Capacity.Memory(), v.Spec.Unschedulable, true
				}
			}
			return v.Status.Capacity.Cpu().MilliValue(), v.Status.Capacity.Memory(), v.Spec.Unschedulable, false
		}
	}
	return -1, nil, false, false

}

func GetNodeMetrics(nodeName string) (float64, float64, bool, bool){
	clientsetMetrics := clientMetricsSetup()
	nodeMetricsList, err := clientsetMetrics.MetricsV1beta1().NodeMetricses().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		log.Printf("Error in getting nodes list : %s\n", err)
	}
	for _, n := range nodeMetricsList.Items {
		if nodeName == n.Name {
			cpu, memory, unschedulable, spotOrNoSchedule := GetNodeCapacity(nodeName)
			memoryFloat := float64(memory.Value()) / (1024.0 * 1024.0)
			nodeCpuUsage := n.Usage.Cpu().MilliValue()
			nodeMemoryUsage := float64(n.Usage.Memory().Value()) / (1024.0 * 1024)
			return (float64(nodeCpuUsage)/float64(cpu))*100.0, float64(nodeMemoryUsage/memoryFloat) * 100.0, unschedulable, spotOrNoSchedule
		}	
	}
	return 0.0,0.0, false, false

}

func podLabelIsThereInNode(pod *v1.Pod) (string, bool) {
	nodesList := getNodeList()
	var podLabel string
	for _, n := range nodesList.Items {
		fmt.Println("Node: ", n.Name)
		if pod.Labels["app"] != "" {
			podLabel = pod.Labels["app"]
		} else {
			podLabel = pod.Name
		}
		if getPodsInANode(n.Name, podLabel) {
			return n.Name, true
		}
	}
	return "nil", false
}

func (ks *CustomKubeScheduler) findFit(pod *v1.Pod) string {
	node, schedulable := podLabelIsThereInNode(pod)
	if schedulable {
		return node
	}
	return "nil"
}

func (ks *CustomKubeScheduler) bindPod(p *v1.Pod, node string) error {
	err := ks.ClientSet.CoreV1().Pods(p.Namespace).Bind(context.Background(), &v1.Binding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      p.Name,
			Namespace: p.Namespace,
		},
		Target: v1.ObjectReference{
			APIVersion: "v1",
			Kind:       "Node",
			Name:       node,
		},
	}, metav1.CreateOptions{})
	return err
}

func (ks *CustomKubeScheduler) emitEvent(p *v1.Pod, message string) error {
	timestamp := time.Now().UTC()
	_, err := ks.ClientSet.CoreV1().Events(p.Namespace).Create(context.Background(), &v1.Event{
		Count:          1,
		Message:        message,
		Reason:         "Scheduled",
		LastTimestamp:  metav1.NewTime(timestamp),
		FirstTimestamp: metav1.NewTime(timestamp),
		Type:           "Normal",
		Source: v1.EventSource{
			Component: "customScheduler",
		},
		InvolvedObject: v1.ObjectReference{
			Kind:      "Pod",
			Name:      p.Name,
			Namespace: p.Namespace,
			UID:       p.UID,
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: p.Name + "-",
		},
	}, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	return nil
}

func clientSetup() *kubernetes.Clientset {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	kubeconfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{})
	config, err := kubeconfig.ClientConfig()
	if err != nil {
		log.Printf("Error in new client config: %s\n", err)
	}
	clientset := kubernetes.NewForConfigOrDie(config)
	return clientset

}

func clientMetricsSetup() *metrics.Clientset {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	kubeconfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{})
	config, err := kubeconfig.ClientConfig()
	if err != nil {
		log.Printf("Error in new client config: %s\n", err)
	}
	clientsetMetrics, err := metrics.NewForConfig(config)
	if err != nil {
		log.Printf("Error in new client metrics config: %s\n", err)
	}
	return clientsetMetrics

}

func getNodeList() *v1.NodeList {
	clientset := clientSetup()
	nodes, err := clientset.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		fmt.Printf("Error in getting nodes list: %s\n", err)
	}
	return nodes
}



func getPodsInANode(nodeName, podLabel string) bool {
	clientset := clientSetup()

	pods, err := clientset.CoreV1().Pods("").List(context.Background(), metav1.ListOptions{FieldSelector: "spec.nodeName=" + nodeName})
	if err != nil {
		log.Printf("Error in getting pods list : %s\n", err)
	}
	for _, p := range pods.Items {
		cpuPercent, memoryPercent, nodeUnschedulable, _ := GetNodeMetrics(nodeName)
		if nodeUnschedulable {//|| spotOrNoSchedule{
			return false
		}else if memoryPercent > 40.0 && cpuPercent > 7.0 {
			return false
		}else if p.Labels["app"] == podLabel  {
				return false
		}
		
	}
	return true
}

func (ks *CustomKubeScheduler) Run(quit chan struct{}) {
	wait.Until(ks.schedulePod, 0, quit)
}

func main() {
	fmt.Println("I'm a scheduler!")

	rand.Seed(time.Now().Unix())

	podQueue := make(chan *v1.Pod, 300)
	defer close(podQueue)

	quit := make(chan struct{})
	defer close(quit)

	scheduler := NewScheduler(podQueue, quit)
	scheduler.Run(quit)
}
