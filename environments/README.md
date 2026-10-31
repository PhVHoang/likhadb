# Rendered environments

Kargo creates and maintains one generated manifest per environment:

```text
environments/
├── dev/manifests.yaml
├── staging/manifests.yaml
└── production/manifests.yaml
```

These files are committed directly to `main` by the Kargo promotion task. Do not
edit them manually. Human-maintained Kubernetes configuration belongs under
`deploy/`; a promotion renders the selected overlay and replaces the applicable
generated manifest.

An environment directory does not exist until its first successful promotion.
Its Argo CD Application may report a missing source path before that point.
