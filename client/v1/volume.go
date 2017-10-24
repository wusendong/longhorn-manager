// Copyright 2016 The prometheus-operator Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v1

import (
	"encoding/json"

	"github.com/pkg/errors"

	extensionsobj "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

// constants for longhorh crd
const (
	OperatorVersion     = "v1"
	Group               = "longhorn.rancher.com"
	LonghornVolumesKind = "LonghornVolume"
	LonghornVolumeName  = "longhorn-volumes"
)

type VolumesGetter interface {
	Volumes(namespace string) VolumeInterface
}

var _ VolumeInterface = &volumes{}

type VolumeInterface interface {
	Create(*Volume) (*Volume, error)
	Get(name string, opts metav1.GetOptions) (*Volume, error)
	Update(*Volume) (*Volume, error)
	Delete(name string, options *metav1.DeleteOptions) error
	List(opts metav1.ListOptions) (runtime.Object, error)
	Watch(opts metav1.ListOptions) (watch.Interface, error)
	DeleteCollection(dopts *metav1.DeleteOptions, lopts metav1.ListOptions) error
}

type volumes struct {
	restClient rest.Interface
	client     *dynamic.ResourceClient
	ns         string
}

func newVolumes(r rest.Interface, c *dynamic.Client, namespace string) *volumes {
	return &volumes{
		r,
		c.Resource(
			&metav1.APIResource{
				Kind:       LonghornVolumesKind,
				Name:       LonghornVolumeName,
				Namespaced: true,
			},
			namespace,
		),
		namespace,
	}
}

func (a *volumes) Create(o *Volume) (*Volume, error) {
	ua, err := UnstructuredFromVolume(o)
	if err != nil {
		return nil, err
	}

	ua, err = a.client.Create(ua)
	if err != nil {
		return nil, err
	}

	return VolumeFromUnstructured(ua)
}

func (a *volumes) Get(name string, opts metav1.GetOptions) (*Volume, error) {
	obj, err := a.client.Get(name, opts)
	if err != nil {
		return nil, err
	}
	return VolumeFromUnstructured(obj)
}

func (a *volumes) Update(o *Volume) (*Volume, error) {
	ua, err := UnstructuredFromVolume(o)
	if err != nil {
		return nil, err
	}

	cura, err := a.Get(o.Name, metav1.GetOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "unable to get current version for update")
	}
	ua.SetResourceVersion(cura.ObjectMeta.ResourceVersion)

	ua, err = a.client.Update(ua)
	if err != nil {
		return nil, err
	}

	return VolumeFromUnstructured(ua)
}

func (a *volumes) Delete(name string, options *metav1.DeleteOptions) error {
	return a.client.Delete(name, options)
}

func (a *volumes) List(opts metav1.ListOptions) (runtime.Object, error) {
	l, _ := labels.Parse(opts.LabelSelector)
	selector, err := fields.ParseSelector(opts.FieldSelector)
	if err != nil {
		return nil, err
	}
	req := a.restClient.Get().
		Namespace(a.ns).
		Resource(LonghornVolumeName).
		// VersionedParams(&options, api.ParameterCodec)
		FieldsSelectorParam(selector).LabelsSelectorParam(l)

	b, err := req.DoRaw()
	if err != nil {
		return nil, err
	}
	var p VolumeList
	return &p, json.Unmarshal(b, &p)
}

func (a *volumes) Watch(opts metav1.ListOptions) (watch.Interface, error) {
	l, _ := labels.Parse(opts.LabelSelector)
	selector, err := fields.ParseSelector(opts.FieldSelector)
	r, err := a.restClient.Get().
		Prefix("watch").
		Namespace(a.ns).
		Resource(LonghornVolumeName).
		// VersionedParams(&options, api.ParameterCodec).
		FieldsSelectorParam(selector).LabelsSelectorParam(l).
		Stream()
	if err != nil {
		return nil, err
	}
	return watch.NewStreamWatcher(&volumeDecoder{
		dec:   json.NewDecoder(r),
		close: r.Close,
	}), nil

}

func (a *volumes) DeleteCollection(dopts *metav1.DeleteOptions, lopts metav1.ListOptions) error {
	return a.client.DeleteCollection(dopts, lopts)
}

// VolumeFromUnstructured unmarshals an Volume object from dynamic client's unstructured
func VolumeFromUnstructured(r *unstructured.Unstructured) (*Volume, error) {
	b, err := json.Marshal(r.Object)
	if err != nil {
		return nil, err
	}
	var a Volume
	if err := json.Unmarshal(b, &a); err != nil {
		return nil, err
	}
	a.TypeMeta.Kind = LonghornVolumesKind
	a.TypeMeta.APIVersion = Group + "/" + Version
	return &a, nil
}

// UnstructuredFromVolume marshals an Volume object into dynamic client's unstructured
func UnstructuredFromVolume(a *Volume) (*unstructured.Unstructured, error) {
	a.TypeMeta.Kind = LonghornVolumesKind
	a.TypeMeta.APIVersion = Group + "/" + Version
	b, err := json.Marshal(a)
	if err != nil {
		return nil, err
	}
	var r unstructured.Unstructured
	if err := json.Unmarshal(b, &r.Object); err != nil {
		return nil, err
	}
	return &r, nil
}

type volumeDecoder struct {
	dec   *json.Decoder
	close func() error
}

func (d *volumeDecoder) Close() {
	d.close()
}

func (d *volumeDecoder) Decode() (action watch.EventType, object runtime.Object, err error) {
	var e struct {
		Type   watch.EventType
		Object Volume
	}
	if err := d.dec.Decode(&e); err != nil {
		return watch.Error, nil, err
	}
	return e.Type, &e.Object, nil
}

func NewLonghornVolumeCustomResourceDefinition(labels map[string]string) *extensionsobj.CustomResourceDefinition {
	return &extensionsobj.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name:   LonghornVolumeName + "." + Group,
			Labels: labels,
		},
		Spec: extensionsobj.CustomResourceDefinitionSpec{
			Group:   Group,
			Version: OperatorVersion,
			Scope:   extensionsobj.NamespaceScoped,
			Names: extensionsobj.CustomResourceDefinitionNames{
				Plural: LonghornVolumeName,
				Kind:   LonghornVolumesKind,
			},
		},
	}
}
