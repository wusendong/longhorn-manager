package kvstore

import (
	"encoding/json"
	"regexp"
	"strings"

	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	eCli "github.com/coreos/etcd/client"
	crdCli "github.com/rancher/longhorn-manager/client"
	lv1 "github.com/rancher/longhorn-manager/client/v1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/client-go/rest"

	extensionsobj "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

type CRDBackend struct {
	Servers []string

	kapi eCli.KeysAPI

	crdclient *apiextensionsclient.Clientset
	lcli      *crdCli.Clientset
	namespace string
	prefix    string

	vcli   lv1.VolumeInterface
	setcli lv1.SettingInterface
}

func NewCRDBackend(namespace string, cfg *rest.Config) (*CRDBackend, error) {
	crdclient, err := apiextensionsclient.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	lcli, err := crdCli.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	backend := &CRDBackend{
		crdclient: crdclient,
		lcli:      lcli,
		namespace: namespace,
		vcli:      lcli.LonghornV1().Volumes(namespace),
		setcli:    lcli.LonghornV1().Settings(namespace),
	}
	if err := backend.ensureCRD(); err != nil {
		return nil, err
	}
	return backend, nil
}

func (s *CRDBackend) ensureCRD() error {
	listFuncs := []func(opts metav1.ListOptions) (runtime.Object, error){
		s.lcli.LonghornV1().Settings(s.namespace).List,
		s.lcli.LonghornV1().Volumes(s.namespace).List,
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
		if _, err = s.crdclient.ApiextensionsV1beta1().CustomResourceDefinitions().Create(crd); err != nil && !apierrors.IsAlreadyExists(err) {
			return errors.Wrapf(err, "Creating CRD: %s", crd.Spec.Names.Kind)
		}
	}

	return util.WaitForCRDReady(listFuncs...)
}

func (s *CRDBackend) trimPrefix(key string) string {
	return strings.TrimPrefix(key, s.prefix)
}

var (
	volumebaserRegx       = regexp.MustCompile(keyVolumes + `/(\S+)/` + keyVolumeBase)
	volumeControllerrRegx = regexp.MustCompile(keyVolumes + `/(\S+)/` + keyVolumeInstances + `/` + keyVolumeInstanceController)
	volumeReplicaRegx     = regexp.MustCompile(keyVolumes + `/(\S+)/` + keyVolumeInstances + `/` + keyVolumeInstanceReplicas + `/(\S+)`)
)

func (s *CRDBackend) Create(key string, obj interface{}) (uint64, error) {
	key = s.trimPrefix(key)

	if key == keySettings {
		// 1. settings
		if setting, err := ToSetting(obj); err != nil {
			return 0, err
		} else if _, err := s.setcli.Create(setting); err != nil {
			return 0, err
		}
	} else if strings.HasPrefix(key, keyVolumes) {
		// 2. volumes
		if fields := volumebaserRegx.FindStringSubmatch(key); len(fields) > 1 {
			// 2.2 /longhorn/volumes/vol1/base
			if volume, err := ToVolume(obj); err != nil {
				return 0, err
			} else if _, err := s.vcli.Create(volume); err != nil {
				return 0, err
			}
		} else if fields := volumeControllerrRegx.FindStringSubmatch(key); len(fields) > 1 {
			// 2.3 /longhorn/volumes/vol1/instances/controller
			volumename := fields[1]
			controllerinfo, ok := obj.(*types.ControllerInfo)
			if !ok {
				return 0, errors.Errorf("Mismatch type: %T", obj)
			}
			volume, err := s.vcli.Get(volumename, metav1.GetOptions{})
			if err != nil {
				return 0, errors.Wrapf(err, "volume %v not found", volumename)
			}
			volume.Spec.Controller = controllerinfo
			if _, err := s.vcli.Update(volume); err != nil {
				return 0, errors.Wrapf(err, "update controller failed")
			}
		} else if fields := volumeReplicaRegx.FindStringSubmatch(key); len(fields) > 2 {
			// 2.4 /longhorn/volumes/vol1/instances/replicas/{vol1}-replica-{7d3248ab-0f95-4454}
			volumename := fields[1]
			replicainfo, ok := obj.(*types.ReplicaInfo)
			if !ok {
				return 0, errors.Errorf("Mismatch type: %T", obj)
			}
			volume, err := s.vcli.Get(volumename, metav1.GetOptions{})
			if err != nil {
				return 0, errors.Wrapf(err, "volume %v not found", volumename)
			}
			volume.Spec.Replicas = append(volume.Spec.Replicas, replicainfo)
			if _, err := s.vcli.Update(volume); err != nil {
				return 0, errors.Wrapf(err, "update controller failed")
			}
		}
	} else if strings.HasPrefix(key, keyNodes) {
		// do nothing cause we just use all k8s nodes
	}

	return 0, nil
}

func (s *CRDBackend) Update(key string, obj interface{}, index uint64) (uint64, error) {
	if key == keySettings {
		// 1. settings
		setting, err := s.lcli.LonghornV1().Settings(s.namespace).Get(keySettings, metav1.GetOptions{})
		if err != nil {
			return 0, errors.Wrapf(err, "setting not found")
		}
		info, ok := obj.(types.SettingsInfo)
		if !ok {
			return 0, errors.Errorf("Mismatch type: %T", obj)
		}
		setting.Spec.BackupTarget = info.BackupTarget
		if _, err := s.lcli.LonghornV1().Settings(s.namespace).Update(setting); err != nil {
			return 0, err
		}
	} else if strings.HasPrefix(key, keyNodes) {
		// 2. volumes
		if fields := volumebaserRegx.FindStringSubmatch(key); len(fields) > 1 {
			// 2.2 /longhorn/volumes/vol1/base
			volumename := fields[1]
			volumeinfo, ok := obj.(*types.VolumeInfo)
			if !ok {
				return 0, errors.Errorf("Mismatch type: %T", obj)
			}
			volume, err := s.vcli.Get(volumename, metav1.GetOptions{})
			if err != nil {
				return 0, errors.Wrapf(err, "volume %v not found", volumename)
			}
			volume.Spec.Volume = volumeinfo
			if _, err := s.vcli.Update(volume); err != nil {
				return 0, errors.Wrapf(err, "update controller failed")
			}
		} else if fields := volumeControllerrRegx.FindStringSubmatch(key); len(fields) > 1 {
			// 2.3 /longhorn/volumes/vol1/instances/controller
			volumename := fields[1]
			controllerinfo, ok := obj.(*types.ControllerInfo)
			if !ok {
				return 0, errors.Errorf("Mismatch type: %T", obj)
			}
			volume, err := s.vcli.Get(volumename, metav1.GetOptions{})
			if err != nil {
				return 0, errors.Wrapf(err, "volume %v not found", volumename)
			}
			volume.Spec.Controller = controllerinfo
			if _, err := s.vcli.Update(volume); err != nil {
				return 0, errors.Wrapf(err, "update controller failed")
			}
		} else if fields := volumeReplicaRegx.FindStringSubmatch(key); len(fields) > 2 {
			// 2.4 /longhorn/volumes/vol1/instances/replicas/{vol1}-replica-{7d3248ab-0f95-4454}
			volumename := fields[1]
			replicainfo, ok := obj.(*types.ReplicaInfo)
			if !ok {
				return 0, errors.Errorf("Mismatch type: %T", obj)
			}
			volume, err := s.vcli.Get(volumename, metav1.GetOptions{})
			if err != nil {
				return 0, errors.Wrapf(err, "volume %v not found", volumename)
			}
			volume.Spec.Replicas = append(volume.Spec.Replicas, replicainfo)
			if _, err := s.vcli.Update(volume); err != nil {
				return 0, errors.Wrapf(err, "update controller failed")
			}
		}
	} else if strings.HasPrefix(key, keyNodes) {
		// do nothing cause we just use all k8s nodes
	}
	return 0, nil
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
