---
title: Security
bookCollapseSection: false
weight: 8
aliases:
- /security.html
- /security/index.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Security

Apache Flink is a distributed stream and batch processing framework that executes user-supplied code across a cluster of machines. Because Flink runs arbitrary submitted code by design, its security model depends on controlling *who* can reach the cluster, not on sandboxing *what* submitted code is allowed to do.

This page describes Flink's threat model: the actors it distinguishes, the trust boundary between them, the attack surface that is in and out of scope for vulnerability reports, the architectural limitations users must account for, and how to report a vulnerability. It covers both Apache Flink and the Flink Kubernetes Operator.

## Apache Flink

### Actors and Trust Levels

Flink distinguishes the actors below. Capabilities granted to a trusted actor are intentional and are **not** vulnerabilities.

| Actor | Trust level | Capabilities |
|---|---|---|
| Cluster / deployment operator | Fully trusted | Configures, deploys, and operates the cluster. Controls configuration, secrets, and the host environment. |
| Job submitter (arbitrary code) | Trusted to run arbitrary code | A party permitted to submit jobs as code -- JARs, DataStream/Table programs, or UDFs (REST API, CLI). Submitted code can spawn processes, open connections, and read or write local files. This is intended behavior, as these submitters are trusted. |
| SQL / declarative submitter | Constrained capability | A party permitted to submit only SQL statements (for example through the SQL Gateway), without the ability to register arbitrary code or JARs. They are *not* expected to be able to execute arbitrary code, so an escape from the SQL interface to arbitrary code execution (for example code-generation injection) is a privilege escalation, and a vulnerability. |
| Data source / upstream producer | Trust varies; often a different actor | Produces the data Flink reads (for example records in a Kafka topic). May be controlled by a different, untrusted party than the job submitter. Flink executing code embedded in this data (for example via unsafe deserialization) is a vulnerability where Flink offers no option to prevent it. |
| Untrusted network actor | Untrusted | Any party that should not be permitted to reach Flink's network interfaces. This is the actor the deployment must keep out. |

**Flink performs no user authentication of its own.** Whether a party is a trusted job submitter or an untrusted outsider is determined by the network controls, the optional mutual TLS, and any authenticating proxy the operator places in front of Flink -- not by Flink itself.

**Access to observability and management channels means access to secrets and data.** Anyone who can read Flink's logs, metrics, Web UI, or REST API can potentially see the credentials Flink uses and the data it is processing. Treat access to these channels as equivalent to access to that data and those secrets, and restrict it accordingly. Leakage through them is an operator concern, not a vulnerability -- the exception being code execution triggered by the data Flink reads (see below).

### Trust Boundary

Flink's security model is built around one explicit trust boundary: **the cluster operator and job submitters are trusted; the cluster's network interfaces must not be reachable by untrusted parties**.

Flink executes submitted code unconditionally. A job can spawn processes, open network connections, read and write local files, and perform any operation the operating system permits. This is intentional -- restricting what user code can do would prevent legitimate use cases. **Flink is not a sandbox.**

The diagram below shows where the boundary sits. Everything inside the cluster network is mutually trusted; the boundary is the network edge, which the operator is responsible for closing to untrusted actors.

{{< mermaid >}}
graph LR
    submitter(["Job submitter (trusted)"])
    producer(["Data producer (untrusted data)"])
    attacker(["Untrusted network actor"])

    subgraph Trusted cluster network
        rest["REST API"]
        gw["SQL Gateway"]
        ui["Web Dashboard"]
        blob["BLOB server"]
        jm["JobManager"]
        tm["TaskManagers (run submitted code)"]
        rest --> jm
        gw --> jm
        ui --> jm
        blob --> jm
        jm --> tm
    end

    ext[("External systems: state backend, sources, sinks")]

    submitter -->|submits jobs| rest
    attacker -.->|blocked by network controls| rest
    producer -->|source data| tm
    tm --> ext
{{< /mermaid >}}

### Attack Surface and Security Boundary Reference

The table below is intended for security researchers and enterprise security teams evaluating Flink. **In scope** means a finding should be reported as a vulnerability (see [Reporting a Vulnerability](#reporting-a-vulnerability)). **Out of scope** means the behavior is intentional and follows from the trust model above.

Flink performs no built-in user authentication, and ships without encryption enabled. An unsecured cluster that is simply reachable by an untrusted party is therefore a **deployment error, not a vulnerability** -- see [Deployment Requirements](#deployment-requirements). The scenarios below assume that distinction: they describe flaws that exist *despite* a correct deployment.

| Component / surface | Example scenario | Boundary | Notes |
|---|---|---|---|
| REST API | Bypass of mutual TLS (client certificate) authentication when it is enabled | **In scope** | Report it |
| REST API | Path traversal or unauthorized file read/upload | **In scope** | Report it (historically CVE-2020-17518, CVE-2020-17519) |
| SQL interface / code generation | SQL or expression injection that escapes the SQL interface to arbitrary code execution | **In scope** | Report it (historically CVE-2026-35194); a SQL-only submitter is not expected to run arbitrary code |
| Input / source data | Code execution triggered by deserializing malicious input data (for example via Kryo or Java serialization), where Flink offers no option to prevent it | **In scope** | Report it; Flink should provide configuration to disable input-data code execution |
| Metrics reporters / JMX | Remote code execution via a reporter endpoint | **In scope** | Report it (historically CVE-2020-1960) |
| BLOB server | Unauthorized artifact upload or retrieval | **In scope** | Report it |
| Web Dashboard | Cross-site scripting (XSS) in the Web UI | **In scope** | Report it |
| History Server | Path traversal or access beyond what it is intended to serve | **In scope** | Report it |
| Internal RPC / data plane | Unauthenticated access to the JobManager &harr; TaskManager RPC or data-exchange plane from outside the cluster network | **In scope** | Report it; must be protected by network controls and SSL/TLS |
| Submitted job (arbitrary code) | Remote code execution via a job submitted as a JAR, DataStream/Table program, or UDF | **Out of scope** | By design -- these submitters run arbitrary code (escaping a SQL-only interface to remote code execution is in scope, above) |
| Submitted job | Spawning processes, opening connections, or reading/writing files from within a running job | **Out of scope** | By design |
| Connectors / UDFs / catalogs / PyFlink | Code execution via a connector, UDF, Hive/JDBC catalog, or PyFlink by a submitter authorized to provide code | **Out of scope** | By design -- submitted code is trusted |
| Logs / metrics / Web UI / REST API | Exposure of credentials or processed data to a party that already has access to these channels | **Out of scope** | By design -- restrict access to these channels (see [Known Limitations](#known-limitations)) |
| External systems | Security of external systems Flink connects to, or the lifecycle of credentials used to reach them | **Out of scope** | Operator responsibility |
| Session cluster | Lack of isolation between jobs sharing a session cluster | **Out of scope** | See [Known Limitations](#known-limitations) |

### Deployment Requirements

Flink clusters must not be exposed to the public internet. Access to all Flink network interfaces must be restricted to trusted principals via network-level controls (firewalls, security groups, VPN).

Flink provides no built-in user authentication or authorization on its network interfaces. The REST endpoint can be encrypted with SSL/TLS and optionally protected with mutual (client-certificate) authentication (`security.ssl.rest.enabled`, `security.ssl.rest.authentication-enabled`); anything beyond that -- user/password, tokens, or role-based access -- must be provided by an **authenticating proxy** placed in front of Flink. The Flink documentation recommends binding the REST endpoint to the loopback (or pod-local) interface and fronting it with a side-car proxy such as Envoy or NGINX. The SQL Gateway likewise has no built-in authentication and must be fronted the same way. SSL/TLS is **disabled by default**; until these controls are added, a Flink deployment is unauthenticated and unencrypted on its network interfaces.

Flink does not manage the security of the external systems it connects to. Through its delegation token framework, Flink can obtain and renew temporary tokens on the operator's behalf, but the underlying long-lived credentials remain an operator responsibility.

## Flink Kubernetes Operator

The [Flink Kubernetes Operator](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/) is the standard deployment mechanism for Flink on Kubernetes. Its security model extends the Flink model above -- both apply when the operator is in use.

### Actors and Trust Levels

| Actor | Trust level | Capabilities |
|---|---|---|
| Kubernetes cluster administrator | Fully trusted | Installs and configures the operator and defines its RBAC. |
| Flink custom resource applier | Trusted to run arbitrary code | Any principal allowed to apply `FlinkDeployment` or `FlinkSessionJob` resources. Equivalent to a job submitter, and effectively inherits the operator's permissions (pod creation, in-namespace secret access) because the operator acts on their behalf. Governed entirely by Kubernetes RBAC. |
| Operator service account | Trusted (cluster-scoped) | Trusted by the Kubernetes control plane to manage Flink resources, by default across namespaces. |

### Trust Boundary

The Kubernetes RBAC layer replaces direct cluster access as the authentication mechanism. Any principal with permission to apply Flink custom resources (`FlinkDeployment`, `FlinkSessionJob`) is the equivalent of an authenticated Flink user -- they can submit arbitrary jobs with full execution trust. The operator's own service account is trusted by the Kubernetes control plane with cluster-scoped permissions.

### Security Boundary Reference

| Component / surface | Example scenario | Boundary | Notes |
|---|---|---|---|
| Admission webhook | Webhook bypass allowing malformed or malicious resources to be applied | **In scope** | Report it |
| Operator | A defect letting a principal act on resources outside the namespaces or tenancy the operator's RBAC and admission control are designed to enforce | **In scope** | Report it (a confused deputy beyond the operator's intended design) |
| Custom resource applier | A `FlinkDeployment`/`FlinkSessionJob` applier gaining the operator's effective permissions (pod creation, in-namespace secret access) | **Out of scope** | By design -- govern via Kubernetes RBAC |
| Custom resource spec / status | Credential exposure via spec, status, events, or exceptions | **Out of scope** | Happens easily -- treat custom resources and their status as sensitive; manage secrets through Kubernetes secrets |
| Operator logs / metrics | Information disclosure via operator logs or metrics | **Out of scope** | Restrict access to these channels |
| Job submission | A principal with custom resource apply permission submitting a malicious job | **Out of scope** | By design -- same trust model as job submission |
| Cluster RBAC | RBAC misconfiguration by the cluster administrator | **Out of scope** | Operator responsibility |

### Deployment Requirements

Apply least-privilege principles to the operator's service account and restrict it to the minimum required permissions; the operator's Helm chart supports both namespaced and cluster-scoped RBAC. The admission webhook, when enabled, uses TLS as required by Kubernetes. Credentials in Flink custom resource specifications flow through Kubernetes secrets and must be managed according to your organization's secret management policies. Restrict Flink custom resource apply permissions to trusted principals.

## Known Limitations

The following are intentional architectural constraints, not vulnerabilities. Operators must account for them when deploying Flink.

- **Flink is not a sandbox.** Submitted code runs with the full privileges of the JobManager and TaskManager processes.
- **No isolation between jobs in a session cluster.** Jobs sharing a session cluster share the same processes and can affect one another. Use per-job or application clusters when isolation between workloads is required.
- **The internal cluster network is trusted.** Once inside the cluster network boundary, components (JobManager, TaskManagers, RPC, data exchange) trust each other. Network-level isolation of the cluster is the operator's responsibility.
- **External system security is out of Flink's control.** Connectors, state backends, and catalogs depend on credentials and access controls that the operator manages.
- **Observability and management channels expose secrets and data.** Anyone with access to logs, metrics, the Web UI, or the REST API can potentially read the credentials Flink uses and the data it processes. Restrict access to these channels accordingly.
- **Input data is processed, not trusted as code.** Where Flink can be configured to avoid executing code embedded in input data (for example unsafe deserialization), enabling that configuration is the operator's responsibility; where no such option exists, that is a vulnerability (see the attack-surface table).

## Security Updates

This table is the project's first-party record of fixed vulnerabilities in Apache Flink and its sub-projects. CVE assignment and coordinated disclosure for all Apache projects are handled by the [ASF Security Team](https://www.apache.org/security/), which acts as the CVE Numbering Authority (CNA). The following databases mirror these records and are useful for automated scanning, but do not replace this table:

- [OSV](https://osv.dev) -- package-aware, used by Dependabot, Snyk, and osv-scanner
- [NVD](https://nvd.nist.gov) -- NIST's CVE database

<table class="table">
	<thead>
		<tr>
			<th style="width: 20%">CVE ID</th>
			<th style="width: 30%">Affected Flink versions</th>
			<th style="width: 50%">Notes</th>
		</tr>
	</thead>
	<tr>
		<td>
			<a href="https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2020-1960">CVE-2020-1960</a>
		</td>
		<td>
			1.1.0 to 1.1.5, 1.2.0 to 1.2.1, 1.3.0 to 1.3.3, 1.4.0 to 1.4.2, 1.5.0 to 1.5.6, 1.6.0 to 1.6.4, 1.7.0 to 1.7.2, 1.8.0 to 1.8.3, 1.9.0 to 1.9.2, 1.10.0
		</td>
		<td>
			Users are advised to upgrade to Flink 1.9.3 or 1.10.1 or later versions or remove the port parameter from the reporter configuration (see advisory for details).
		</td>
	</tr>
	<tr>
		<td>
			<a href="https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2020-17518">CVE-2020-17518</a>
		</td>
		<td>
			1.5.1 to 1.11.2
		</td>
		<td>
			<a href="https://github.com/apache/flink/commit/a5264a6f41524afe8ceadf1d8ddc8c80f323ebc4">Fixed in commit a5264a6f41524afe8ceadf1d8ddc8c80f323ebc4</a> <br>
			Users are advised to upgrade to Flink 1.11.3 or 1.12.0 or later versions.
		</td>
	</tr>
	<tr>
		<td>
			<a href="https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2020-17519">CVE-2020-17519</a>
		</td>
		<td>
			1.11.0, 1.11.1, 1.11.2
		</td>
		<td>
			<a href="https://github.com/apache/flink/commit/b561010b0ee741543c3953306037f00d7a9f0801">Fixed in commit b561010b0ee741543c3953306037f00d7a9f0801</a> <br>
			Users are advised to upgrade to Flink 1.11.3 or 1.12.0 or later versions.
		</td>
	</tr>
	<tr>
		<td>
			<a href="https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2023-41834">CVE-2023-41834</a>
		</td>
		<td>
			Flink Stateful Functions 3.1.0, 3.1.1, 3.2.0
		</td>
		<td>
			<a href="https://github.com/apache/flink-statefun/commit/b06c0a23a5a622d48efc8395699b2e4502bd92be">Fixed in commit b06c0a23a5a622d48efc8395699b2e4502bd92be</a> <br>
			Users are advised to upgrade to Flink Stateful Functions 3.3.0 or later versions.
		</td>
	</tr>
	<tr>
		<td>
			<a href="https://www.cve.org/CVERecord?id=CVE-2026-35194">CVE-2026-35194</a>
		</td>
		<td>
			1.15.0 through 1.20.x and 2.0.0 through 2.x
		</td>
		<td>
			<a href="https://github.com/apache/flink/commit/64007b131d689158af90ca1c1b71b018129a85c5">Fixed in commits 64007b131d689158af90ca1c1b71b018129a85c5</a>, <a href="https://github.com/apache/flink/commit/e7c0d17074dc0dc9e102a072f11bf0de09ba01a5">e7c0d17074dc0dc9e102a072f11bf0de09ba01a5</a> and <a href="https://github.com/apache/flink/commit/9b2a11268dc8b4e6ea5a604dca0ea27f0fee3ed8">9b2a11268dc8b4e6ea5a604dca0ea27f0fee3ed8</a> <br>
			Users are advised to upgrade to Flink 1.20.4, 2.0.2, 2.1.2 or 2.2.1.
		</td>
	</tr>
</table>

## Reporting a Vulnerability

If you discover a vulnerability that falls **in scope** per the boundaries above, please report it privately through one of the following channels:

- **Apache Security Team:** [security@apache.org](mailto:security@apache.org) -- preferred for CVE assignment and coordinated disclosure
- **Flink PMC (private):** [private@flink.apache.org](mailto:private@flink.apache.org) -- for direct discussion with the Flink PMC

Please do not open public GitHub issues or pull requests for security vulnerabilities.
