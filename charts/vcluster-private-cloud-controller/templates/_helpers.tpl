{{/*
Chart name.
*/}}
{{- define "vpcc.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Fully qualified release name.
*/}}
{{- define "vpcc.fullname" -}}
{{- if contains (include "vpcc.name" .) .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "vpcc.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{/*
Common labels.
*/}}
{{- define "vpcc.labels" -}}
app.kubernetes.io/name: {{ include "vpcc.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end -}}

{{/*
ServiceAccount name.
*/}}
{{- define "vpcc.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "vpcc.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
Image reference; tag falls back to the chart appVersion.
*/}}
{{- define "vpcc.image" -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- end -}}

{{/*
Effective --cluster-name for a cluster entry. Scope: dict with "cluster".
*/}}
{{- define "vpcc.clusterName" -}}
{{- default .cluster.name .cluster.clusterName -}}
{{- end -}}
