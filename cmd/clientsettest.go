package main

import (
	"fmt"
	"log"
	"path/filepath"
	"reflect"
	"test/util"
	"time"

	"github.com/Sirupsen/logrus"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/workqueue"

	"github.com/pkg/errors"
	lclient "github.com/rancher/longhorn-manager/client"
	lv1 "github.com/rancher/longhorn-manager/client/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	// "github.com/rancher/longhorn-manager/types"
	// "github.com/rancher/longhorn-manager/util"
	// "k8s.io/client-go/kubernetes"
	// "k8s.io/client-go/tools/clientcmd"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/labels"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	// apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiv1 "k8s.io/client-go/pkg/api/v1"
)

func main() {
	config, err := clientcmd.BuildConfigFromFlags("", "/Users/dong/vagrant/k8s-playground/admin.conf")
	if err != nil {
		panic(err.Error())
	}

	kcli, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}
	nodes, err := kcli.Core().Pods("default").List(metav1.ListOptions{LabelSelector: labels.SelectorFromSet(labels.Set(map[string]string{
		"app": "longhorn-manager",
	})).String()})
	if err != nil {
		panic(err)
	}
	if err != nil {
		if apierrors.IsNotFound(err) {
			return
		}
		panic(err)
	}
	ret := []string{}
	for _, node := range nodes.Items {
		for _, cond := range node.Status.Conditions {
			if cond.Type == apiv1.PodReady && cond.Status == apiv1.ConditionTrue {
				ret = append(ret, filepath.Join("s.prefix, keyNodes", node.Spec.NodeName))
				break
			}
		}
	}

	util.PrintJSON(ret)
	// util.PrintJSON(nodes)

	// lcli, err := lclient.NewForConfig(config)
	// if err != nil {
	// 	panic(err.Error())
	// }
	// crdclient, err := apiextensionsclient.NewForConfig(config)
	// if err != nil {
	// 	panic("instantiating apiextensions client failed")
	// }

	fmt.Println("watching")

	// watcher, err := lcli.LonghornV1().Volumes("default").Watch(metav1.ListOptions{})
	// if err != nil {
	// 	panic(err.Error())
	// }

	// go func() {
	// 	eventChan := watcher.ResultChan()
	// 	for event := range eventChan {
	// 		util.PrintJSON(event.Type)
	// 	}
	// 	fmt.Println("watcher stoped")
	// }()

	// lo, err := New(config)
	// if err != nil {
	// 	fmt.Fprint(os.Stderr, "instantiating alertmanager controller failed: ", err)
	// 	return
	// }
	// ctx, cancel := context.WithCancel(context.Background())
	// wg, ctx := errgroup.WithContext(ctx)
	// wg.Go(func() error { return lo.Run(ctx.Done()) })
	// term := make(chan os.Signal)
	// signal.Notify(term, os.Interrupt, syscall.SIGTERM)

	// select {
	// case <-term:
	// 	logrus.Info("Received SIGTERM, exiting gracefully...")
	// case <-ctx.Done():
	// }
	// cancel()
	// if err := wg.Wait(); err != nil {
	// 	logrus.Info("Unhandled error received. Exiting...", "err", err)
	// 	return
	// }

	fmt.Println("stoped")

}

type Operator struct {
	kclient   kubernetes.Interface
	lcli      *lclient.Clientset
	crdclient apiextensionsclient.Interface
	logger    log.Logger

	alrtInf cache.SharedIndexInformer
	ssetInf cache.SharedIndexInformer

	queue workqueue.RateLimitingInterface
}

const (
	resyncPeriod = 5 * time.Minute
)

func New(config *rest.Config) (*Operator, error) {

	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, errors.Wrap(err, "instantiating kubernetes client failed")
	}

	lcli, err := lclient.NewForConfig(config)
	if err != nil {
		return nil, errors.Wrap(err, "instantiating longhorn client failed")
	}
	crdclient, err := apiextensionsclient.NewForConfig(config)
	if err != nil {
		return nil, errors.Wrap(err, "instantiating apiextensions client failed")
	}

	o := &Operator{
		kclient:   client,
		lcli:      lcli,
		crdclient: crdclient,
		queue:     workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "alertmanager"),
	}
	o.alrtInf = cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListFunc:  o.lcli.LonghornV1().Volumes("default").List,
			WatchFunc: o.lcli.LonghornV1().Volumes("default").Watch,
		},
		&lv1.Volume{}, resyncPeriod, cache.Indexers{},
	)
	o.alrtInf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    o.handleVolumeAdd,
		DeleteFunc: o.handleVolumeDelete,
		UpdateFunc: o.handleVolumeUpdate,
	})

	return o, nil
}

// Run the controller.
func (c *Operator) Run(stopc <-chan struct{}) error {
	defer c.queue.ShutDown()

	errChan := make(chan error)
	go func() {
		v, err := c.kclient.Discovery().ServerVersion()
		if err != nil {
			errChan <- errors.Wrap(err, "communicating with server failed")
			return
		}
		logrus.Info("connection established", "cluster-version", v)

		if err := c.createCRDs(); err != nil {
			errChan <- err
			return
		}
		errChan <- nil
	}()

	select {
	case err := <-errChan:
		if err != nil {
			return err
		}
		logrus.Info("CRD API endpoints ready")
	case <-stopc:
		return nil
	}

	go c.worker()

	go c.alrtInf.Run(stopc)

	<-stopc
	return nil
}

func (c *Operator) worker() {
	for c.processNextWorkItem() {
	}
}

func (c *Operator) processNextWorkItem() bool {
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(key)

	err := c.sync(key.(string))
	if err == nil {
		c.queue.Forget(key)
		return true
	}

	utilruntime.HandleError(errors.Wrap(err, fmt.Sprintf("Sync %q failed", key)))
	c.queue.AddRateLimited(key)

	return true
}

func (c *Operator) sync(key string) error {
	obj, exists, err := c.alrtInf.GetIndexer().GetByKey(key)
	if err != nil {
		return err
	}
	if !exists {
		util.PrintJSON("exists")
	}
	util.PrintJSON(obj)
	return nil
}

func (c *Operator) createCRDs() error {
	return nil
}

func (c *Operator) keyFunc(obj interface{}) (string, bool) {
	k, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		logrus.Info("creating key failed", "err", err)
		return k, false
	}
	return k, true
}

// enqueue adds a key to the queue. If obj is a key already it gets added directly.
// Otherwise, the key is extracted via keyFunc.
func (c *Operator) enqueue(obj interface{}) {
	if obj == nil {
		return
	}

	key, ok := obj.(string)
	if !ok {
		key, ok = c.keyFunc(obj)
		if !ok {
			return
		}
	}

	c.queue.Add(key)
}
func (c *Operator) handleVolumeAdd(obj interface{}) {
	key, ok := c.keyFunc(obj)
	if !ok {
		return
	}

	logrus.Info("Volume added", "key", key)
	c.enqueue(key)
}

func (c *Operator) handleVolumeDelete(obj interface{}) {
	key, ok := c.keyFunc(obj)
	if !ok {
		return
	}

	logrus.Info("Volume deleted", "key", key)
	c.enqueue(key)
}

func (c *Operator) handleVolumeUpdate(old, cur interface{}) {
	if reflect.DeepEqual(old, cur) {
		fmt.Println("----- equal!")
		return
	}
	key, ok := c.keyFunc(cur)
	if !ok {
		return
	}

	logrus.Info("Volume updated", "key", key)
	c.enqueue(key)
}
