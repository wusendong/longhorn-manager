package manager

import (
	"fmt"
	"log"
	"time"

	"github.com/Sirupsen/logrus"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"github.com/pkg/errors"
	lclient "github.com/rancher/longhorn-manager/client"
	lv1 "github.com/rancher/longhorn-manager/client/v1"
	"github.com/rancher/longhorn-manager/util"
	extensionsobj "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
)

type Operator struct {
	kclient   kubernetes.Interface
	lcli      *lclient.Clientset
	crdclient apiextensionsclient.Interface
	logger    log.Logger

	alrtInf cache.SharedIndexInformer
	ssetInf cache.SharedIndexInformer

	queue workqueue.RateLimitingInterface

	stopc chan struct{}

	namespace string
	eventChan chan Event
}

const (
	resyncPeriod = 5 * time.Minute
)

func NewOperator(namespace string, eventChan chan Event) (*Operator, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, errors.Wrap(err, "cannot connect to k8s, may be not in k8s cluster")
	}

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
		eventChan: eventChan,
		namespace: namespace,
		stopc:     make(chan struct{}),
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
func (c *Operator) Run() error {
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
	case <-c.stopc:
		return nil
	}

	go c.worker()

	go c.alrtInf.Run(c.stopc)

	logrus.Info("longhorn operator ready")
	<-c.stopc
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
		return nil
	}
	v, ok := obj.(*lv1.Volume)
	if !ok {
		return errors.Errorf("Miss match type :%T", obj)
	}
	c.eventChan <- Event{EventTypeNotify, v.GetName()}
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
	ol, ok := old.(*lv1.Volume)
	if !ok {
		return
	}
	cu, ok := cur.(*lv1.Volume)
	if !ok {
		return
	}
	if volumestateequal(ol, cu) {
		return
	}

	key, ok := c.keyFunc(cur)
	if !ok {
		return
	}

	logrus.Info("Volume updated", "key", key)
	c.enqueue(key)
}

func volumestateequal(old, cur *lv1.Volume) bool {
	if old.Spec.Volume.DesireState != cur.Spec.Volume.DesireState {
		return false
	}
	if old.Status == cur.Status {
		return true
	}
	return true
}

func (c *Operator) createCRDs() error {
	listFuncs := []func(opts metav1.ListOptions) (runtime.Object, error){
		c.lcli.LonghornV1().Settings(c.namespace).List,
		c.lcli.LonghornV1().Volumes(c.namespace).List,
	}
	exists, err := util.CrdExists(listFuncs...)
	if err == nil && exists {
		return nil
	}

	crds := []*extensionsobj.CustomResourceDefinition{
		lv1.NewLonghornSettingCustomResourceDefinition(lv1.GetLonghornLables()),
		lv1.NewLonghornVolumeCustomResourceDefinition(lv1.GetLonghornLables()),
	}

	for _, crd := range crds {
		if _, err = c.crdclient.ApiextensionsV1beta1().CustomResourceDefinitions().Create(crd); err != nil && !apierrors.IsAlreadyExists(err) {
			return errors.Wrapf(err, "Creating CRD: %s", crd.Spec.Names.Kind)
		}
	}

	return util.WaitForCRDReady(listFuncs...)
}

func (c *Operator) ShutDown() {
	close(c.stopc)
}
