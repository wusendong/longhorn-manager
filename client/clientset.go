package longhorn

import (
	"github.com/rancher/longhorn-manager/client/v1"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/flowcontrol"
)

var _ Interface = &Clientset{}

type Interface interface {
	LonghornV1() v1.LonghornV1Interface
}

type Clientset struct {
	*v1.LonghornV1Client
}

func (c *Clientset) LonghornV1() v1.LonghornV1Interface {
	if c == nil {
		return nil
	}
	return c.LonghornV1Client
}

func NewForConfig(c *rest.Config) (*Clientset, error) {
	configShallowCopy := *c
	if configShallowCopy.RateLimiter == nil && configShallowCopy.QPS > 0 {
		configShallowCopy.RateLimiter = flowcontrol.NewTokenBucketRateLimiter(configShallowCopy.QPS, configShallowCopy.Burst)
	}
	var cs Clientset
	var err error

	cs.LonghornV1Client, err = v1.NewForConfig(v1.Group, &configShallowCopy)
	if err != nil {
		return nil, err
	}

	return &cs, nil
}
