{{/*
Expand the name of the chart.
*/}}
{{- define "rustcrawl.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Fully qualified app name.
*/}}
{{- define "rustcrawl.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "rustcrawl.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{/*
Common labels.
*/}}
{{- define "rustcrawl.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "rustcrawl.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
Selector labels.
*/}}
{{- define "rustcrawl.selectorLabels" -}}
app.kubernetes.io/name: {{ include "rustcrawl.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Container image reference.
*/}}
{{- define "rustcrawl.image" -}}
{{- printf "%s:%s" .Values.image.repository (.Values.image.tag | default .Chart.AppVersion) -}}
{{- end -}}

{{/*
The crawler command line: seeds, then args, then extra args, then the flags we
always force in-cluster (`--no-save` keeps the root filesystem read-only, and
`-q` runs headless without the terminal dashboard).
*/}}
{{- define "rustcrawl.args" -}}
{{- range .Values.crawl.seeds }}
- {{ . | quote }}
{{- end }}
{{- range .Values.crawl.args }}
- {{ . | quote }}
{{- end }}
{{- range .Values.crawl.extraArgs }}
- {{ . | quote }}
{{- end }}
- "--no-save"
- "-q"
{{- end -}}

{{/*
The shared Job spec (also embedded inside the CronJob's jobTemplate). Callers
must include this with the correct nindent for their nesting depth.
*/}}
{{- define "rustcrawl.jobSpec" -}}
backoffLimit: {{ .Values.job.backoffLimit }}
activeDeadlineSeconds: {{ .Values.job.activeDeadlineSeconds }}
ttlSecondsAfterFinished: {{ .Values.job.ttlSecondsAfterFinished }}
template:
  metadata:
    labels:
      {{- include "rustcrawl.selectorLabels" . | nindent 6 }}
  spec:
    restartPolicy: {{ .Values.job.restartPolicy }}
    {{- with .Values.imagePullSecrets }}
    imagePullSecrets:
      {{- toYaml . | nindent 6 }}
    {{- end }}
    securityContext:
      {{- toYaml .Values.podSecurityContext | nindent 6 }}
    containers:
      - name: {{ .Chart.Name }}
        image: {{ include "rustcrawl.image" . | quote }}
        imagePullPolicy: {{ .Values.image.pullPolicy }}
        args:
          {{- include "rustcrawl.args" . | nindent 10 }}
        {{- with .Values.env }}
        env:
          {{- toYaml . | nindent 10 }}
        {{- end }}
        securityContext:
          {{- toYaml .Values.securityContext | nindent 10 }}
        resources:
          {{- toYaml .Values.resources | nindent 10 }}
        volumeMounts:
          - name: tmp
            mountPath: /tmp
          {{- if .Values.persistence.enabled }}
          - name: data
            mountPath: {{ .Values.persistence.mountPath }}
          {{- end }}
    volumes:
      - name: tmp
        emptyDir: {}
      {{- if .Values.persistence.enabled }}
      - name: data
        persistentVolumeClaim:
          claimName: {{ .Values.persistence.existingClaim | default (printf "%s-data" (include "rustcrawl.fullname" .)) }}
      {{- end }}
    {{- with .Values.nodeSelector }}
    nodeSelector:
      {{- toYaml . | nindent 6 }}
    {{- end }}
    {{- with .Values.affinity }}
    affinity:
      {{- toYaml . | nindent 6 }}
    {{- end }}
    {{- with .Values.tolerations }}
    tolerations:
      {{- toYaml . | nindent 6 }}
    {{- end }}
{{- end -}}
