package longhorn

import (
	"testing"

	longhorncli "github.com/rancher/longhorn-manager/client/v1"

	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestCRD(t *testing.T) {
	config, err := clientcmd.BuildConfigFromFlags("", "/Users/dong/vagrant/k8s-playground/admin.conf")
	if err != nil {
		panic(err.Error())
	}

	_, err = kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	crdcli, err := NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}
	crdclient, err := apiextensionsclient.NewForConfig(config)
	if err != nil {
		panic("instantiating apiextensions client failed")
	}
	volumecrd := longhorncli.NewLonghornVolumeCustomResourceDefinition(nil)
	result, err := crdclient.ApiextensionsV1beta1().CustomResourceDefinitions().Create(volumecrd)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		panic(err.Error())
	}
	// defer func() {
	// 	err = crdclient.ApiextensionsV1beta1().CustomResourceDefinitions().Delete(result.GetObjectMeta().GetName(), &metav1.DeleteOptions{})
	// 	if err != nil {
	// 		panic(err.Error())
	// 	}
	// }()
	util.PrintJSON(result)

	util.WaitForCRDReady(crdcli.LonghornV1().Volumes("default").List)

	volume := longhorncli.Volume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "vol1",
		},
		Spec: longhorncli.VolumeSpec{
			Volume: &types.VolumeInfo{
				Name: "vol1",
				Size: 20 * 1024 * 1024 * 1024,
			},
		},
	}
	create, err := crdcli.LonghornV1().Volumes("default").Create(&volume)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		panic(err.Error())
	}
	util.PrintJSON(create)

	for {
		if _, err := crdcli.LonghornV1().Volumes("default").List(metav1.ListOptions{}); err == nil {
			break
		}
	}

	list, err := crdcli.LonghornV1().Volumes("default").List(metav1.ListOptions{})
	if err != nil {
		panic(err.Error())
	}
	util.PrintJSON(list.(*longhorncli.VolumeList))
	vreslut, err := crdcli.LonghornV1().Volumes("default").Get("vol1", metav1.GetOptions{})
	if err != nil {
		panic(err.Error())
	}
	util.PrintJSON(vreslut)

	if err := util.WaitForCRDReady(crdcli.LonghornV1().Settings("default").List); err != nil {
		panic(err.Error())
	}
	setreslut, err := crdcli.LonghornV1().Settings("default").List(metav1.ListOptions{})
	if err != nil {
		panic(err.Error())
	}
	util.PrintJSON(setreslut)

}
