# Generative AI use policy

*ReckonDB project policy for contributors. Author: Raf Lefever, BEAM Campus / Macula. Belgium.*

## Scope and purpose

This policy governs the use of generative AI (GenAI) tools by contributors to the ReckonDB project, including the project maintainer, paid contractors funded under any grant, and external contributors submitting pull requests. It complies with NLnet's Policy on the use of Generative Artificial Intelligence for NLnet-funded projects (nlnet.nl/foundation/policies/generativeAI) and is the public artefact that NLnet's policy recommends each project publish.

The policy applies to all work delivered into the ReckonDB repository: source code, tests, documentation, design specifications, research notes, blog posts, conference materials, and proposals submitted on behalf of the project.

## Permitted uses of GenAI assistance

GenAI tools may be used for the following categories of work, with provenance disclosed when substantive (see "Disclosure" below):

- **Documentation drafting and editing.** README, API reference, migration guides, tutorial content, blog posts, and other prose documentation may be drafted with AI assistance, then reviewed and edited by a human contributor before merge.
- **Code review prompts and exploratory programming.** Asking an AI for review feedback on a draft pull request, or for an exploratory implementation sketch that the contributor then rewrites by hand, is permitted.
- **Mechanical assistance.** Character counting, file rendering (e.g. PDF generation), formatting, syntax-checking, and similar non-substantive transforms.
- **Test scaffolding and boilerplate.** AI-generated test scaffolding may be used as a starting point, provided the contributor verifies that each test exercises the behaviour it claims to exercise.
- **Comparison and prior-art surveys.** Drafting comparisons against external systems, with citations verified by a human contributor before publication.

## Categories explicitly excluded from GenAI assistance

The following categories of work are **not** to be produced by GenAI tools, regardless of subsequent human editing:

- **Cryptographic primitives.** Implementations of encryption, key derivation, signing, and key-wrap primitives are written by hand against published specifications (RFCs, peer-reviewed papers). AI may not propose or transcribe primitive implementations.
- **Threat-model design.** The threat model in the protocol specification, the trust-base scoping, and the formal characterisation of guaranteed-versus-not-guaranteed properties are produced by a human contributor with cryptographic-engineering competence.
- **Security-critical code paths.** Capability validation, epoch-rotation logic, key-destruction logic, and consensus-bound operations in the data layer are written by hand and reviewed by a human contributor before merge.
- **External-review preparation packets.** The packet submitted to external cryptographic reviewers is composed by the contributor, not by an AI assistant, so that the reviewer's questions reach the human author directly.
- **Formal protocol specifications under review.** Protocol-specification text submitted to standards bodies or to external review is authored by a human contributor.

## Disclosure

For substantive GenAI assistance, contributors record provenance in the form NLnet's policy specifies: model used, dates and times of prompts, the prompts themselves, and the unedited model output. The provenance log is preserved by the contributor and is available to the project maintainer and to NLnet on request.

For mechanical or trivial AI use (formatting, syntax checks, boilerplate transforms), provenance logging is not required. Contributors use judgement; when in doubt, log.

Pull requests with substantive AI-assisted content state this in the pull-request description. Commit messages do not require AI-disclosure footers; the pull-request description is the canonical record.

## Review and update

This policy is reviewed annually, at the close of each NLnet milestone, and whenever NLnet's Generative AI policy is updated. The policy is versioned in the repository alongside the source code; substantive changes are announced in the project changelog.

## Contact

Questions about this policy may be directed to the project maintainer via the ReckonDB repository issue tracker, or by email at the address listed in the project README.
