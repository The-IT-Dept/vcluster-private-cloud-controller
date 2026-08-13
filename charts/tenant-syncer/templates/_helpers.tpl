{{- define "tenant-syncer.name" -}}
{{- printf "%s-syncer" .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "tenant-syncer.labels" -}}
app.kubernetes.io/name: tenant-syncer
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- with .Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{- define "tenant-syncer.selectorLabels" -}}
app.kubernetes.io/name: tenant-syncer
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
