Business Requirements for an Integrated Member Experience Command Center for Insurance Client

1. Objective
Transform a one-time, retrospective analysis of their 100M+ member interactions into a live, integrated nervous system that continuously monitors, explains, and improves member experience while quantifying real financial impact and driving daily action, not annual insights.
This implementation must enable executives to see where experience and cost are leaking, understand why, and activate fixes across digital, call center, and operations in near real time.
2. Scope & Data Foundation
The solution must integrate and correlate disparate experience, transactional, and operational data on the Databricks Lakehouse, including:
• Claims & Prior Authorization
• Call Center Data (ACD, IVR, transcripts, handle time)
• Voice of Customer (Qualtrics surveys, sentiment, effort)
• Digital Interaction Data (logins, failures, abandonment)
• Enrollment & Plan Data
• Pharmacy & Benefits Data
• Socio-demographic & Member Attributes
All data must be linkable at the member journey level, enabling cross-channel journey reconstruction and analysis.
3. Core Capabilities (What the System Must Do)
A. Live Journey Monitoring
• Continuously monitor end-to-end member journeys across:
• Digital self-service
• Call center interactions
• Pharmacy and benefits changes
• Detect journey breakdowns (e.g., digital → call leakage, repeat demand) in near real time.
B. Financial Quantification
• Translate experience failures into dollar impact, not just CX scores:
• Avoidable call volume
• Cost per call
• Repeat demand
• Annualized and near-term savings potential
• Allow leaders to model 5–10% reductions and see financial upside in weeks, not years.
C. Integrated Experience Command Center
Deliver a role-based, live dashboard (not static reporting) that includes:
• Digital → Call Failures
• Live count and cost of digital journeys that convert to calls
• Breakdown by topic, plan type, tenure, and segment
• Repeat Calls & Emotion
• Surging repeat demand and negative sentiment
• Correlation to communication gaps, process failures, or agent behaviors
• “Money on the Table”
• Continuously refreshed Top 5 opportunities
• Call volume, cost, trend, and primary levers (digital, comms, ops)
• Member Journey Playback
• Cross-channel timeline view of an individual journey
• Digital failures, calls, sentiment, claims/auth context
4. Agentic AI Requirements (From Insight to Action)
Insight Scout Agent
• Runs daily on the Lakehouse
• Automatically detects:
• Topic spikes
• Repeat call pockets
• Sentiment and effort drops
• Produces human-readable executive briefings, e.g.
“Digital bill-pay failures increased 14% for MAPD Segment X, driving ~$Y in incremental call cost.”
Next-Best-Action Agent
• For each high-value issue, proposes specific, executable actions, such as:
• Digital copy or flow updates
• IVR messaging changes
• FAQ or communication fixes
• Training or QA interventions
• Prioritizes actions by:
• Estimated cost savings
• CX improvement
• Speed to impact
5. Sample Executive Questions the System Must Answer Live
Cost & Prioritization
• Which three journeys are driving the most avoidable call cost right now?
• If we fix one issue this quarter, which saves the most money and why?
• Where are repeat calls that should be one-and-done?
Experience & Risk
• Which member segments are experiencing the highest negative emotion?
• Are new members failing more than tenured members—and where?
• Which benefit or cost changes are creating confusion ahead of AEP?
Digital Effectiveness
• Which digital journeys are leaking into the call center—and at what rate?
• Where do members abandon online flows and immediately call?
• Which “self-service” journeys are not actually self-service?
Operational Alignment
• Are agent behaviors impacting sentiment and repeat calls?
• Which issues are best solved by digital fixes vs training vs communication?
6. Success Criteria
The POC is successful if it:
• Converts historical insight into a live, operational system
• Demonstrates measurable cost reduction (5–10%) in targeted journeys
• Provides executives with one source of truth across experience, cost, and operations
• Moves the organization from knowing what’s broken to fixing it every week
This is not another analytics dashboard.
It is a living nervous system watching member experience, quantifying financial impact, and using agentic AI to tell teams what to fix next, powered by the Databricks platform the client already owns.
This is the full concept for the  implementation. However for presales in the meeting with client next Monday what kind of demo can we prepare with synthetic data.
Some of the current problem areas identified are:
1. Digital friction is massive: millions of failed digital‑first journeys, 1.2M+ calls after failed digital attempts, 2.4M ID card and 2.2M billing calls.
2. Call center strain is real: 23.5% repeat calls, 32.6% negative emotion calls, pharmacy calls dominating volume.
3. Communication & benefit confusion drive cost and churn: hundreds of thousands of benefit‑change and cost calls, long handle times, repeat demand.
