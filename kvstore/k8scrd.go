package kvstore

import (
	"encoding/json"
	"fmt"

	"github.com/rancher/longhorn-manager/client/monitoring"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	eCli "github.com/coreos/etcd/client"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/client-go/rest"

	extensionsobj "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type CRDBackend struct {
	Servers []string

	kapi eCli.KeysAPI

	crdcli *apiextensionsclient.Clientset
	mcli   *monitoring.Clientset
}

func NewCRDBackend(cfg *rest.Config) (*CRDBackend, error) {

	// TODO: create crds first, and create a crd client for each resources
	crdclient, err := apiextensionsclient.NewForConfig(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "instantiating apiextensions client failed")
	}

	volumeCRD := NewLonghornVolumeCustomResourceDefinition(nil)
	crdclient.ApiextensionsV1beta1().CustomResourceDefinitions().Create(volumeCRD)

	mclient, err := monitoring.NewForConfig(Group, cfg)
	if err != nil {
		return nil, err
	}
	backend := &CRDBackend{
		crdcli: crdclient,
		mcli:   mclient,
	}

	return backend, nil
}

func (s *CRDBackend) Create(key string, obj interface{}) (uint64, error) {
	value, err := json.Marshal(obj)
	if err != nil {
		return 0, err
	}
	resp, err := s.kapi.Create(context.Background(), key, string(value))
	if err != nil {
		return 0, err
	}
	return resp.Node.ModifiedIndex, nil
}

func (s *CRDBackend) Update(key string, obj interface{}, index uint64) (uint64, error) {
	if index == 0 {
		return 0, fmt.Errorf("kvstore index cannot be 0")
	}
	value, err := json.Marshal(obj)
	if err != nil {
		return 0, err
	}
	resp, err := s.kapi.Set(context.Background(), key, string(value), &eCli.SetOptions{
		PrevIndex: index,
	})
	if err != nil {
		return 0, err
	}
	return resp.Node.ModifiedIndex, nil
}

func (s *CRDBackend) IsNotFoundError(err error) bool {
	return eCli.IsKeyNotFound(err)
}

func (s *CRDBackend) Get(key string, obj interface{}) (uint64, error) {
	resp, err := s.kapi.Get(context.Background(), key, nil)
	if err != nil {
		return 0, err
	}
	node := resp.Node
	if node.Dir {
		return 0, errors.Errorf("invalid node %v is a directory",
			node.Key)
	}

	if err := json.Unmarshal([]byte(node.Value), obj); err != nil {
		return 0, errors.Wrap(err, "fail to unmarshal json")
	}
	return node.ModifiedIndex, nil
}

func (s *CRDBackend) Keys(prefix string) ([]string, error) {
	resp, err := s.kapi.Get(context.Background(), prefix, nil)
	if err != nil {
		if eCli.IsKeyNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	if !resp.Node.Dir {
		return nil, errors.Errorf("invalid node %v is not a directory",
			resp.Node.Key)
	}

	ret := []string{}
	for _, node := range resp.Node.Nodes {
		ret = append(ret, node.Key)
	}
	return ret, nil
}

func (s *CRDBackend) Delete(key string) error {
	_, err := s.kapi.Delete(context.Background(), key, &eCli.DeleteOptions{
		Recursive: true,
	})
	if err != nil {
		if eCli.IsKeyNotFound(err) {
			return nil
		}
		return err
	}
	return nil
}

func NewLonghornVolumeCustomResourceDefinition(labels map[string]string) *extensionsobj.CustomResourceDefinition {
	return &extensionsobj.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name:   LonghornManagerName + "." + Group,
			Labels: labels,
		},
		Spec: extensionsobj.CustomResourceDefinitionSpec{
			Group:   Group,
			Version: OperatorVersion,
			Scope:   extensionsobj.NamespaceScoped,
			Names: extensionsobj.CustomResourceDefinitionNames{
				Plural: LonghornManagerName,
				Kind:   LonghornManagerKind,
			},
		},
	}
}

// longhorh crd object
const (
	OperatorVersion     = "v1"
	Group               = "rancher.com"
	LonghornManagerName = "longhorn-managers"
	LonghornManagerKind = "LonghornManager"
)
