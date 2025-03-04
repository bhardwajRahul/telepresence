{{/*
Expand the name of the chart.
*/}}
{{- define "telepresence.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Traffic Manager deployment/service name - as of v2.20.3, must be "traffic-manager" to align with code base.
*/}}
{{- define "traffic-manager.name" -}}
{{- $name := default "traffic-manager" }}
{{- print $name }}
{{- end -}}

{{- /*
Traffic Manager Namespace
*/}}
{{- define "traffic-manager.namespace" -}}
{{- if .Values.isCI }}
{{- print "ambassador" }}
{{- else }}
{{- printf "%s" .Release.Namespace }}
{{- end }}
{{- end -}}

{{- /*
traffic-manager.namespace-list extracts the list of namespace names from the namespaces variable.
For backward compatibility, it will also consider names from the deprecated managerRbac.namespaces.
It's an error if namespaces and managerRbac.namespaces both have values.
*/}}
{{- define "private.namespace-list" }}
  {{- $names := .Values.namespaces }}
  {{- if .Values.managerRbac.namespaces }}
    {{- if $names }}
      {{- fail "namespaces and managerRbac.namespaces are mutually exclusive" }}
    {{- end }}
    {{- $names = .Values.managerRbac.namespaces }}
  {{- end }}
  {{- range $names }}
    {{- if not (regexMatch `^[a-z0-9]([a-z0-9-]*[a-z0-9])?$` .) }}
      {{- fail (printf "namespace %q is not a valid RFC 1123 namespace name" .) }}
    {{- end }}
  {{- else }}
    {{ $names = list }}
  {{- end }}
  {{- toJson (uniq ($names)) }}
{{- end }}

{{- define "private.namespaceSelector" }}
  {{- $labels := list }}
  {{- $matches := list }}
  {{- with .Values.namespaceSelector }}
    {{- with .matchLabels }}
      {{- $labels = . }}
    {{- end }}
    {{- with .matchExpressions }}
      {{- $matches = . }}
    {{- end }}
  {{- end }}
  {{- with fromJsonArray (include "private.namespace-list" $) }}
    {{- if (or $labels $matches) }}{{ fail "namespaces and namespaceSelector are mutually exclusive" }}{{ end }}
    {{- $matches = append $matches (dict "key" "kubernetes.io/metadata.name" "operator" "In" "values" .) }}
  {{- end }}
  {{- $selector := dict }}
  {{- with $labels }}
    {{- $selector = set $selector "matchLabels" . }}
  {{- end }}
  {{- with $matches }}
    {{- $selector = set $selector "matchExpressions" . }}
  {{- end }}
  {{- toJson $selector }}
{{- end }}

{{- /*
traffic-manager.namespaceSelector extracts the selector to use when selecting namespaces.

This selector will either include the namespaceSelector variable or include namespaces returned by the
private.namespace-list definition. It will fail if both of them have values.

The selector will default to the deprecated agentInjector.webhook.namespaceSelector when neither the namespaceSelector
nor the private.namespace-list definition has any value.

A selector can be dynamic or static. This in turn controls if telepresence is "cluster-wide" or "namespaced". A dynamic
selector requires cluster-wide access for the traffic-manager, and only a static selector can serve as base when
installing Role/RoleBinding pairs.

A selector is considered static if it meets the following conditions:
- The selector must have exactly one element in the `matchLabels` or the `matchExpression`
  list (if the element is in the `matchLabels` list, it is normalized into "key in [value]").
- The element must meet the following criteria:
  The `key` of the match expression must be "kubernetes.io/metadata.name".
  The `operator` of the match expression must be "In" (case sensitive).
  The `values` list of the match expression must contain at least one value.
*/}}
{{- define "traffic-manager.namespaceSelector" }}
  {{- $selector := mustFromJson (include "private.namespaceSelector" $) }}
  {{- $legacy := false }}
  {{- if not $selector }}
    {{- with .Values.agentInjector.webhook.namespaceSelector }}
      {{- $legacy = true }}
      {{- $selector = . }}
    {{- end }}
  {{- end }}
  {{- if not (or $legacy (fromJsonArray (include "traffic-manager.namespaces" $))) }}
    {{- /*Ensure that his dynamic selector rejects "kube-system" and "kube-node-lease" */}}
    {{- $mes := $selector.matchExpressions }}
    {{- if not $mes }}
      {{- $mes = list }}
    {{- end }}
    {{- $selector = set $selector "matchExpressions" (append $mes
      (dict "key" "kubernetes.io/metadata.name" "operator" "NotIn" "values" (list "kube-system" "kube-node-lease")))
    }}
  {{- end }}
  {{- toJson $selector }}
{{- end }}

{{- /*
traffic-manager.namespaced will yield the string "true" if the traffic-manager.namespaceSelector that is static.
*/}}
{{- define "traffic-manager.namespaced" }}
  {{- if fromJsonArray (include "traffic-manager.namespaces" $) }}
    {{- true }}
  {{- end }}
{{- end }}

{{- /*
traffic-manager.namespaces will return a list of namespaces, provided that the traffic-manager.namespaceSelector is static.
*/}}
{{- define "traffic-manager.namespaces" }}
  {{- $namespaces := list }}
  {{- with mustFromJson (include "private.namespaceSelector" $) }}
    {{- if and .matchExpressions (eq (len .matchExpressions) 1) (not .matchLabels) }}
      {{- with index .matchExpressions 0}}
        {{- if (and (eq .operator "In") (eq .key "kubernetes.io/metadata.name")) }}
          {{- $namespaces = .values }}
        {{- end }}
      {{- end }}
    {{- end }}
    {{- if and .matchLabels (eq (len .matchLabels) 1) (not .matchExpressions) }}
      {{- with get .matchLabels "kubernetes.io/metadata.name" }}
        {{- $namespaces = list . }}
      {{- end }}
    {{- end }}
  {{- end }}
  {{- toJson $namespaces }}
{{- end }}

{{- /*
Create chart name and version as used by the chart label.
*/}}
{{- define "telepresence.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- /*
Common labels
*/}}
{{- define "telepresence.labels" -}}
{{ include "telepresence.selectorLabels" $ }}
helm.sh/chart: {{ include "telepresence.chart" $ }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- /* This value is intentionally undocumented -- it's used by the telepresence binary to determine ownership of the release */}}
{{- if .Values.createdBy }}
app.kubernetes.io/created-by: {{ .Values.createdBy }}
{{- else }}
app.kubernetes.io/created-by: {{ .Release.Service }}
{{- end }}
{{- end }}

{{- /*
Selector labels
*/}}
{{- define "telepresence.selectorLabels" -}}
app: traffic-manager
telepresence: manager
{{- end }}

{{- /*
Client RBAC name suffix
*/}}
{{- define "telepresence.clientRbacName" -}}
{{ printf "%s-%s" (include "telepresence.name" $) (include "traffic-manager.namespace" $) }}
{{- end -}}

{{- /*
RBAC rules required to create an intercept in a namespace; excludes any rules that are always cluster wide.
*/}}
{{- define "telepresence.clientRbacInterceptRules" -}}
{{- /* Mandatory. Controls namespace access command completion experience */}}
- apiGroups: [""]
  resources: ["pods"]
  verbs: ["get","list"] {{- /* "list" is only necessary if the client should be able to gather the pod logs */}}
- apiGroups: [""]
  resources: ["pods/log"]
  verbs: ["get"]
{{- /* All traffic will be routed via the traffic-manager unless a portforward can be created directly to a pod */}}
- apiGroups: [""]
  resources: ["pods/portforward"]
  verbs: ["create"]
{{- if and .Values.clientRbac .Values.clientRbac.ruleExtras }}
{{ template "clientRbac-ruleExtras" . }}
{{- end }}
{{- end }}

{{/*
Kubernetes version
*/}}
{{- define "kube.version.major" }}
{{- $version := regexFind "^[0-9]+" .Capabilities.KubeVersion.Major -}}
{{- printf "%s" $version -}}
{{- end -}}

{{- define "kube.version.minor" }}
{{- $version := regexFind "^[0-9]+" .Capabilities.KubeVersion.Minor -}}
{{- printf "%s" $version -}}
{{- end -}}
