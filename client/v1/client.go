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
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/pkg/api"
	"k8s.io/client-go/rest"
)

var Version = "v1"

type LonghornV1Interface interface {
	RESTClient() rest.Interface
	VolumesGetter
	SettingsGetter
}

type LonghornV1Client struct {
	restClient    rest.Interface
	dynamicClient *dynamic.Client
}

func (c *LonghornV1Client) Volumes(namespace string) VolumeInterface {
	return newVolumes(c.restClient, c.dynamicClient, namespace)
}
func (c *LonghornV1Client) Settings(namespace string) SettingInterface {
	return newSettings(c.restClient, c.dynamicClient, namespace)
}

func (c *LonghornV1Client) RESTClient() rest.Interface {
	return c.restClient
}

func NewForConfig(apiGroup string, c *rest.Config) (*LonghornV1Client, error) {
	config := *c
	SetConfigDefaults(apiGroup, &config)
	client, err := rest.RESTClientFor(&config)
	if err != nil {
		return nil, err
	}

	dynamicClient, err := dynamic.NewClient(&config)
	if err != nil {
		return nil, err
	}

	return &LonghornV1Client{client, dynamicClient}, nil
}

func SetConfigDefaults(apiGroup string, config *rest.Config) {
	config.GroupVersion = &schema.GroupVersion{
		Group:   apiGroup,
		Version: Version,
	}
	config.APIPath = "/apis"
	config.NegotiatedSerializer = serializer.DirectCodecFactory{CodecFactory: api.Codecs}
	return
}

func GetLonghornLables(args ...string) map[string]string {
	m := map[string]string{"app": "longhorn"}
	for i := 0; i < len(args); i = i + 2 {
		m[args[i]] = args[i+1]
	}
	return m
}
