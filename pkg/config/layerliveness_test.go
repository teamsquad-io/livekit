// Copyright 2026 TeamSquad
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

package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLayerLivenessConfig(t *testing.T) {
	t.Run("off by default", func(t *testing.T) {
		conf, err := NewConfig("", true, nil, nil)
		require.NoError(t, err)
		require.False(t, conf.LayerLiveness.Metrics)
		require.False(t, conf.LayerLiveness.APIFilter)
	})

	t.Run("the metric switch stands alone", func(t *testing.T) {
		conf, err := NewConfig("layer_liveness:\n  metrics: true\n", true, nil, nil)
		require.NoError(t, err)
		require.True(t, conf.LayerLiveness.Metrics)
		require.False(t, conf.LayerLiveness.APIFilter)
	})

	t.Run("the API filter refuses to boot as a silent no-op", func(t *testing.T) {
		_, err := NewConfig("layer_liveness:\n  api_filter: true\n", true, nil, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "enable_psrpc_for_get_list_participants")
	})

	t.Run("the API filter with its prerequisite", func(t *testing.T) {
		conf, err := NewConfig(
			"api:\n  enable_psrpc_for_get_list_participants: true\nlayer_liveness:\n  api_filter: true\n",
			true, nil, nil,
		)
		require.NoError(t, err)
		require.True(t, conf.LayerLiveness.APIFilter)
	})
}
