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
	LonghornSettingsKind = "LonghornSetting"
	LonghornSettingName  = "longhornsettings"
)

type SettingsGetter interface {
	Settings(namespace string) SettingInterface
}

var _ SettingInterface = &settings{}

type SettingInterface interface {
	Create(*Setting) (*Setting, error)
	Get(name string, opts metav1.GetOptions) (*Setting, error)
	Update(*Setting) (*Setting, error)
	Delete(name string, options *metav1.DeleteOptions) error
	List(opts metav1.ListOptions) (runtime.Object, error)
	Watch(opts metav1.ListOptions) (watch.Interface, error)
	DeleteCollection(dopts *metav1.DeleteOptions, lopts metav1.ListOptions) error
}

type settings struct {
	restClient rest.Interface
	client     *dynamic.ResourceClient
	ns         string
}

func newSettings(r rest.Interface, c *dynamic.Client, namespace string) *settings {
	return &settings{
		r,
		c.Resource(
			&metav1.APIResource{
				Kind:       LonghornSettingsKind,
				Name:       LonghornSettingName,
				Namespaced: true,
			},
			namespace,
		),
		namespace,
	}
}

func (a *settings) Create(o *Setting) (*Setting, error) {
	ua, err := UnstructuredFromSetting(o)
	if err != nil {
		return nil, err
	}

	ua, err = a.client.Create(ua)
	if err != nil {
		return nil, err
	}

	return SettingFromUnstructured(ua)
}

func (a *settings) Get(name string, opts metav1.GetOptions) (*Setting, error) {
	obj, err := a.client.Get(name, opts)
	if err != nil {
		return nil, err
	}
	return SettingFromUnstructured(obj)
}

func (a *settings) Update(o *Setting) (*Setting, error) {
	ua, err := UnstructuredFromSetting(o)
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

	return SettingFromUnstructured(ua)
}

func (a *settings) Delete(name string, options *metav1.DeleteOptions) error {
	return a.client.Delete(name, options)
}

func (a *settings) List(opts metav1.ListOptions) (runtime.Object, error) {
	l, _ := labels.Parse(opts.LabelSelector)
	selector, err := fields.ParseSelector(opts.FieldSelector)
	if err != nil {
		return nil, err
	}
	req := a.restClient.Get().
		Namespace(a.ns).
		Resource(LonghornSettingName).
		// VersionedParams(&options, api.ParameterCodec)
		FieldsSelectorParam(selector).LabelsSelectorParam(l)

	b, err := req.DoRaw()
	if err != nil {
		return nil, err
	}
	var p SettingList
	return &p, json.Unmarshal(b, &p)
}

func (a *settings) Watch(opts metav1.ListOptions) (watch.Interface, error) {
	l, _ := labels.Parse(opts.LabelSelector)
	selector, err := fields.ParseSelector(opts.FieldSelector)
	r, err := a.restClient.Get().
		Prefix("watch").
		Namespace(a.ns).
		Resource(LonghornSettingName).
		// VersionedParams(&options, api.ParameterCodec).
		FieldsSelectorParam(selector).LabelsSelectorParam(l).
		Stream()
	if err != nil {
		return nil, err
	}
	return watch.NewStreamWatcher(&settingDecoder{
		dec:   json.NewDecoder(r),
		close: r.Close,
	}), nil

}

func (a *settings) DeleteCollection(dopts *metav1.DeleteOptions, lopts metav1.ListOptions) error {
	return a.client.DeleteCollection(dopts, lopts)
}

// SettingFromUnstructured unmarshals an Setting object from dynamic client's unstructured
func SettingFromUnstructured(r *unstructured.Unstructured) (*Setting, error) {
	b, err := json.Marshal(r.Object)
	if err != nil {
		return nil, err
	}
	var a Setting
	if err := json.Unmarshal(b, &a); err != nil {
		return nil, err
	}
	a.TypeMeta.Kind = LonghornSettingsKind
	a.TypeMeta.APIVersion = Group + "/" + Version
	return &a, nil
}

// UnstructuredFromSetting marshals an Setting object into dynamic client's unstructured
func UnstructuredFromSetting(a *Setting) (*unstructured.Unstructured, error) {
	a.TypeMeta.Kind = LonghornSettingsKind
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

type settingDecoder struct {
	dec   *json.Decoder
	close func() error
}

func (d *settingDecoder) Close() {
	d.close()
}

func (d *settingDecoder) Decode() (action watch.EventType, object runtime.Object, err error) {
	var e struct {
		Type   watch.EventType
		Object Setting
	}
	if err := d.dec.Decode(&e); err != nil {
		return watch.Error, nil, err
	}
	return e.Type, &e.Object, nil
}

func NewLonghornSettingCustomResourceDefinition(labels map[string]string) *extensionsobj.CustomResourceDefinition {
	return &extensionsobj.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name:   LonghornSettingName + "." + Group,
			Labels: labels,
		},
		Spec: extensionsobj.CustomResourceDefinitionSpec{
			Group:   Group,
			Version: OperatorVersion,
			Scope:   extensionsobj.NamespaceScoped,
			Names: extensionsobj.CustomResourceDefinitionNames{
				Plural: LonghornSettingName,
				Kind:   LonghornSettingsKind,
			},
		},
	}
}
