# Contributing

- `make verify test` must pass, and `make lint` if you have golangci-lint. Builds are
  offline from `vendor/`: after changing dependencies, run `go mod tidy && go mod vendor`
  and commit `vendor/`.
- Every non-trivial change comes with a test that fails without it.
- A new or changed environment variable updates the README configuration table and
  `.env.example`.
- `docs/DESIGN.md` describes how the code works now; a change in behavior updates it in
  the same pull request.
- Use placeholders, never real values: `jenkins.example.internal`, `example-job`,
  `<S3_BUCKET>`. The real deployment runs inside a compliance boundary; none of its
  hostnames, account IDs, bucket names or credentials belong in this repository.
- Reviews and approvals follow [OWNERS](OWNERS).
