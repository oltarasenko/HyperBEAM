<img src="https://arweave.net/dOpRkKrNNQ4HHebxZlPCo0BWfrjwJ-CEBQs2EPgrwbg" />

Soon.

<!-- toc -->

-   [Development](#development)
-   [Metrics (Grafana and Prometheus)](#metrics-grafana-and-prometheus)

<!-- tocstop -->

## Development

Environment Setup TBD

This repo uses `npm` and `node` to install and execute top level, repo-wide, tooling. So first, you should run `npm i`
at the root to install those dependencies. This will set up git hooks that will help ensure you're following some of the
guidelines outlined in this document:

-   All Erlang files are formatted using [`erlfmt`](https://github.com/WhatsApp/erlfmt)
-   All Commit Messages follow our [Conventional Commits Config](https://www.conventionalcommits.org/en/v1.0.0/)
-   All Markdown files should contain a Table of Contents

Once you've install the top level tooling, each of these conventions are enforced, via git a commit hook, automatically.

## Metrics (Grafana and Prometheus)

In order to access the Grafana dashboard do the following:

```
cd ./metrics
docker-compose up
```

Navigate to Grafana (localhost:3000) (use admin/admin username) and find the Beam dashboard there.
