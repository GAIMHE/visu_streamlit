\documentclass{article}

% if you need to pass options to natbib, use, e.g.:
%     \PassOptionsToPackage{numbers, compress}{natbib}
% before loading neurips_2026

\usepackage[square,numbers]{natbib}
% \usepackage{natbib}
\bibliographystyle{plainnat}

\usepackage[eandd]{neurips_2026}
% the "default" option is equal to the "main" option, which is used for the Main Track with double-blind reviewing.
% 1. "main" option is used for the Main Track
%  \usepackage[main]{neurips_2026}
% 2. "position" option is used for the Position Paper Track
%  \usepackage[position]{neurips_2026}
% 3. "eandd" option is used for the Evaluations & Datasets Track
 % \usepackage[eandd]{neurips_2026}
 % if you need to opt-in for a single-blind submission in the E&D track:
 %\usepackage[eandd, nonanonymous]{neurips_2026}
% 4. "creativeai" option is used for the Creative AI Track
%  \usepackage[creativeai]{neurips_2026}
% 5. "sglblindworkshop" option is used for the Workshop with single-blind reviewing
 % \usepackage[sglblindworkshop]{neurips_2026}
% 6. "dblblindworkshop" option is used for the Workshop with double-blind reviewing
%  \usepackage[dblblindworkshop]{neurips_2026}

% After being accepted, the authors should add "final" behind the track to compile a camera-ready version.
% 1. Main Track
 % \usepackage[main, final]{neurips_2026}
% 2. Position Paper Track
%  \usepackage[position, final]{neurips_2026}
% 3. Evaluations & Datasets Track
 % \usepackage[eandd, final]{neurips_2026}
% 4. Creative AI Track
%  \usepackage[creativeai, final]{neurips_2026}
% 5. Workshop with single-blind reviewing
%  \usepackage[sglblindworkshop, final]{neurips_2026}
% 6. Workshop with double-blind reviewing
%  \usepackage[dblblindworkshop, final]{neurips_2026}
% Note. For the workshop paper template, both \title{} and \workshoptitle{} are required, with the former indicating the paper title shown in the title and the latter indicating the workshop title displayed in the footnote.
% For workshops (5., 6.), the authors should add the name of the workshop, "\workshoptitle" command is used to set the workshop title.
% \workshoptitle{WORKSHOP TITLE}

% "preprint" option is used for arXiv or other preprint submissions
 % \usepackage[preprint]{neurips_2026}

% to avoid loading the natbib package, add option nonatbib:
%    \usepackage[nonatbib]{neurips_2026}

\usepackage[utf8]{inputenc} % allow utf-8 input
\usepackage[T1]{fontenc}    % use 8-bit T1 fonts
\usepackage{hyperref}       % hyperlinks
\usepackage{url}            % simple URL typesetting
\usepackage{graphicx}       % figures
\usepackage{booktabs}       % professional-quality tables
\usepackage{amsfonts}       % blackboard math symbols
\usepackage{nicefrac}       % compact symbols for 1/2, etc.
\usepackage{microtype}      % microtypography
\usepackage{xcolor}         % colors
\usepackage{comment}
% Note. For the workshop paper template, both \title{} and \workshoptitle{} are required, with the former indicating the paper title shown in the title and the latter indicating the workshop title displayed in the footnote. 
\newcommand{\DATASET}{Dataset Name}
\title{\DATASET}

\usepackage{booktabs}
\usepackage{multirow}
\usepackage{array}
\usepackage{makecell}


% The \author macro works with any number of authors. There are two commands
% used to separate the names and addresses of multiple authors: \And and \AND.
%
% Using \And between authors leaves it to LaTeX to determine where to break the
% lines. Using \AND forces a line break at that point. So, if LaTeX puts 3 of 4
% authors names on the first line, and the last on the second line, try using
% \AND instead of \And before the third author name.


\author{%
  David S.~Hippocampus\thanks{Use footnote for providing further information
    about author (webpage, alternative address)---\emph{not} for acknowledging
    funding agencies.} \\
  Department of Computer Science\\
  Cranberry-Lemon University\\
  Pittsburgh, PA 15213 \\
  \texttt{hippo@cs.cranberry-lemon.edu} \\
  % examples of more authors
  % \And
  % Coauthor \\
  % Affiliation \\
  % Address \\
  % \texttt{email} \\
  % \AND
  % Coauthor \\
  % Affiliation \\
  % Address \\
  % \texttt{email} \\
  % \And
  % Coauthor \\
  % Affiliation \\
  % Address \\
  % \texttt{email} \\
  % \And
  % Coauthor \\
  % Affiliation \\
  % Address \\
  % \texttt{email} \\
}


\begin{document}


\maketitle


\begin{abstract}
  
\end{abstract}


\section{Introduction}
presentation of the dataset: exercises + learning pathways + evaluation benchmark
Briefly describe the adressed gap and the contribution

Expliquer l’intérêt du pdv éducatif
Décrire l’intérêt du pdv ML : quelles questions scientifiques, comparer aux autre DS ML pour ED

Find a name for the dataset

\section{Related work}
S’inspirer de la section 2.1 de XES3G5M

describe existing limitations in AI for Ed field
describe the gap in the educational datasets (real students)
describe existing datasets and why they are unsufficient
describe our contribution and how it addressed the gap



\section{The \DATASET~dataset}
\subsection{Collection}
\begin{itemize}
    \item Introduce AdaptivMath \& MIA
    \item How exercises were designed, with their type (give examples in appendices)
    \item Work modes - uniquement MIA groupe de contrôle
    \item Population and classrooms
    \item Student's consent?
\end{itemize}

\subsubsection{Introduce AdaptivMath \& MIA}

% Products of EvidenceB are deployed in 5 counties and count more than 160k users.
% AM counts more than 30k user, deployed in > 1k schools, > 2mln exercises are realized.
% MIA seconde > 70k students, ~ 2.5k schools, 4 mln exercises realized.


Adaptiv’Math (AM) is an adaptive digital learning resource developed as part of the P2IA (Innovation and Artificial Intelligence Partnership) and dedicated to mathematics education for early primary school students (ages 6–8). The platform is grounded in advances in cognitive science and artificial intelligence, enabling learners to develop a robust and intuitive understanding of core mathematical concepts and fundamentals. It includes more than 8000 exercises organized into 7 Modules. AM counts more than 30k user, deployed in > 1k schools, > 2mln exercises are realized. \\
MIA Seconde is digital service provides teachers with pedagogical resources and tools to implement personalized remediation and support pathways in French and mathematics, aiming to strengthen the core competencies of secondary school students. Initiated by the French Ministry of Education and developed by EvidenceB and Docaposte in collaboration with research labs and EdTech partners, it follows a user-centered co-design approach and is supported under the Programme d’Investissements d’Avenir (PIA, Program of Future Inverstments). It contains 24 modules : 16 in French and 8 in maths and more than 20 000 exercises. MIA Seconde is the first AI-based remediation product to be evaluated through a large-scale scientific impact study, involving over 500 high schools across all academic regions in France (more than 1,000 classes and 60,000 students). [SUPERSET USAGE INFO HERE] \\
Both learning solutions are incorporated into an AI driven teaching platform (AITP) that allows to personalize the learning path through a ZPDES algorithm. The platform is designed to complement the standard curriculum, supporting remediation, consolidation, or enrichment. All content is aligned with official educational standards. Students can engage with the platform in the classroom or at home, either independently or in paired mode, depending on the teacher’s instructional strategy. Teachers access a dedicated dashboard that provides detailed insights into student progress, areas of difficulty, and overall performance.\\
The platform includes a hierarchically structured exercise corpus developed by experts in pedagogy and cognitive science, grounded in contemporary research in psychology and cognitive sciences ([12,10] CHECK REFS). The design enables interactive and cognitively informed formats that extend beyond traditional textbook activities. Exercises are organized as interconnected graphs and are structured within interdependent modules, objectives, and activities. Each exercise is associated with an action-driven metacognitive feedback aiming at reinforcing students’ cognitive strategy. A  recommendation engine selects subsequent tasks dynamically based on individual performance, with the objective of maximizing expected learning progress ([9] [CHECK REFS]). This personalization is driven by the ZPDES algorithm, a multi-armed bandit reinforcement learning framework ([5,6] REFS), developed by the Inria FLOWERS team. The algorithm is grounded in Learning Progress theory and computational models of curiosity-driven learning ([17,9] REFS). \\
The platform also offers a “playlist” mode, enabling teachers to assign a predefined set of exercises to students. However, this mode does not support adaptive personalization and is therefore not the default usage. In practice, it was primarily used to constitute control groups in the randomized controlled trial (RCT) conducted for the impact study.


\subsubsection{Exercise design}
All exercises in Adaptiv’Math and MIA Seconde were designed by cognitive science researchers and pedagogical experts, grounded in contemporary findings from cognitive science. Their interaction formats (“gameplays”) are informed by UX research and streamlined design principles to support learning while minimizing distraction.\\
Content is organized hierarchically: modules contain learning objectives, which are subdivided into activities composed of multiple exercises. Within each activity, exercises share the same structure, interaction design, and pedagogical goal, varying only in numerical parameters or surface features.\\
In the present dataset, we share 4 the most commonly used modules representing > 90\% of usage [TO CHECK]: 
\begin{itemize}
    \item 2 modules of MIA : “Relearning number sense” (authored by André Knops, LaPsyDé, Paris-Cité University) \& "Data organization and management; functions" (authored by Valeria Giardino (Institut Jean Nicod – CNRS-ENS-EHES)
    \item 2 modules of Adaptiv’Math: “Numbers and calculations” and “Arithmetic problem solving”  (authored by Emmanuel Sanders, Geneva University), the latter having 3 variations for 3 grades : CP, CE1, CE2. 
\end{itemize}

\textbf{The MIA Module “Relearning Number Sense”}, designed for high school students in a remediation context, comprises 10 objectives, 70 activities, and 1,045 exercises structured within a hierarchical framework. Grounded in a cognitive and developmental approach, the module focuses on strengthening mental representations of numbers through four core competencies—positioning, comparing, ordering, and calculating. Building on prerequisite knowledge (e.g., signed integers and powers of positive integers), the activities target key concepts from lower secondary curricula and systematically explore numerical representations while addressing well-documented cognitive biases (e.g., magnitude, linguistic, and integer biases). \\

The module \textbf{"Data organization and management"} conceptualizes mathematics as a form of heterogeneous reasoning that combines intuitive perceptual skills with higher-level cognition through diverse representational artifacts (e.g., diagrams, graphs, formulas). It builds on students’ spatial intuitions to progressively develop expertise in graph interpretation, symbolic notation, and quantitative reasoning. The module contains 7 objectives, 48 Activities and 482 exercises.


\textbf{Adaptiv’Math Modules 1 and 2}, designed for primary school students, comprise structured sets of exercises grounded in cognitive science and educational psychology. \textbf{Module 1 (“Numbers and Calculations”)} focuses on the progressive development of number sense through a hierarchical sequence of objectives, spanning from non-symbolic quantity processing to symbolic reasoning, arithmetic operations, and early multiplication; task difficulty is systematically controlled via interpretable parameters (e.g., numerical ratios, distances, and perceptual congruency), often introducing biases that require inhibitory control. \textbf{Module “Arithmetic Problem Solving”} targets arithmetic word problem solving, where learners must interpret a textual situation, map it to a mathematical structure, and compute a solution. It is grounded in conceptual change theories, aiming to help students overcome intuitive, experience-based reasoning. Exercises systematically vary the alignment between intuitive interpretations and formal problem structures through three analogy types—substitution (operation–situation mapping), scenario (object relationships), and simulation (operand ordering). Tasks cover both additive and multiplicative situations and are supported by visual representations (e.g., line diagrams, number boxes, rectangle models) to scaffold problem modeling. The module spans three grade levels (Grades 1–3, ages 6–9). Each level follows a consistent format while increasing in numerical complexity: from 10 objectives and 619 exercises at Grade 1, to 16 and 1,156 at Grade 2, and 19 and 1,589 at Grade 3 — enabling fine-grained analysis of students' evolving strategies and misconception.

@TODO : add visu of some gameplays in the annexe

\begin{table}[ht]
\centering
\caption{Comparison of ITS content: MIA vs.\ Adaptiv'Math}
\label{tab:its-comparison}
\renewcommand{\arraystretch}{1.3}
\begin{tabular}{lcc}
\toprule
 & \textbf{MIA} & \textbf{Adaptiv'Math} \\
\midrule
\textbf{Population}         & High school  & Primary school \\
\textbf{Learning objective} & Remediation  & Training \\
\midrule
\multicolumn{3}{l}{\textbf{Module 1}} \\
\quad Name       & Relearning number sense & Numbers and calculations \\
\quad Objectives & 10      & 16 \\
\quad Activities & 70      & 84 \\
\quad Exercises  & 1{,}045 & 2{,}310 \\
\midrule
\multicolumn{3}{l}{\textbf{Module 2}} \\
\quad Name       & \makecell[c]{Data organization \\ \& functions} & \makecell[c]{Arithmetic \\ problem solving} \\
\quad Objectives & 7   & 45 \\
\quad Activities & 48  & 163 \\
\quad Exercises  & 482 & 3{,}364 \\
\midrule
\textbf{Total exercises} & \textbf{1{,}527} & \textbf{5{,}674} \\
\bottomrule
\end{tabular}
\end{table}

\subsubsection{Exercises format}
Each exercise is stored as an individual JSON object, with schemas that are partially shared across interaction modes (\textit{gameplays}) and partially gameplay-specific. Common fields include the problem statement and instruction, while answer representations vary depending on the interaction type: for instance, numerical inputs are used for open-ended responses, whereas categorical indices are used for multiple-choice formats.\\
To facilitate usage across modalities, we provide three aligned representations per exercise: (1) the raw JSON data with gameplay-dependent structure, (2) a normalized textual description of the problem statement and instruction generated via template-based rules specific to each gameplay (or a fallback generic template), and (3) a screenshot of the exercise interface. The JSON additionally specifies whether visual elements are present and whether they are essential to solving the task or purely illustrative, enabling both text-only and vision-language use cases.\\
Each exercise includes the correct answer, candidate options when applicable (e.g., multiple-choice or drag-and-drop), the gameplay type, and associated feedback messages. Feedback is deterministic and depends on response correctness: explanatory feedback is provided for incorrect answers, and motivational feedback for correct answers.\\
Exercises are mapped to a hierarchical prerequisite graph through unique ids. For each level, we provide textual descriptions of targeted skills, activity labels, and expert-defined “targeted difficulties”, which describe the intended cognitive and pedagogical challenges.\\
Finally, exercises are connected to student learning trajectories via their identifiers, allowing reconstruction of sequences of interactions for downstream sequential modeling tasks.\\

\subsubsection{Parametric Graph}

Exercises are structured into a hierarchical graph composed of modules, objectives, and activities. Each module is associated with a distinct directed graph, which is included in the dataset. While no explicit dependencies exist between modules—allowing flexible ordering defined by the teacher—they are typically presented sequentially in order of increasing difficulty within the application.

Within each module, the graph encodes prerequisite relationships between activities, thereby specifying the skill dependencies required to successfully complete associated exercises. This graph serves as the backbone of the ZPDES algorithm, enabling structured progression through the curriculum.

The graph also defines the initial set of accessible activities and the conditions required to unlock or complete subsequent ones. In particular, a new activity is unlocked when a learner achieves a mean score of at least 0.75 over a sequence of exercises (with a constraint of consecutive successes), while an activity is considered mastered and closed at a higher threshold (0.9). These criteria operationalize progression and mastery at a fine-grained level.

Visual representations of these graphs are provided in Appendix XXX. Each visualization is a matrix with objectives as rows and activities as columns, where cell values encode requirements. For instance, in MIA Module 1, the first activity of Objectives 2 and 3 is open by default, and achieving a mean success rate $ \geq 0.75$ on these activities, enables access to the second activity within each objective.

Finally, the initial exercises presented to a student are determined jointly by the graph structure and the outcome of the adaptive placement test.

\subsubsection{Adaptive test}

At the beginning of each module, students complete an adaptive placement test designed to estimate their mastery at the activity level. The test dynamically selects up to approximately 20 questions based on prior responses, maximizing diagnostic efficiency. Importantly, no feedback—neither correctness nor metacognitive cues—is provided during this phase, ensuring unbiased assessment.

This adaptive initialization enables a personalized entry point into the curriculum: rather than starting uniformly, students are positioned at an appropriate level of difficulty. The resulting mastery estimates are used to initialize the ZPDES algorithm, which subsequently recommends exercises within the learner’s zone of proximal development. Notably, adaptive testing is available for all MIA students, while earlier versions of Adaptiv’Math (pre-2024) do not include this feature.

\subsubsection{Learning Paths Collection}

During interaction with the platform, we log detailed traces at the exercise level, including exercise identifiers, associated module, objective, and activity. In contrast to many existing AIEd datasets [REFS], we provide fine-grained temporal data: timestamps corresponding to exercise display and answer validation by the student allow computation of response time, while the interval between answer submission and the next exercise display enables estimation of feedback reading time. These signals offer additional insight into student engagement and learning dynamics.

The dataset also includes rich interaction features. We record the exact student responses—not only correctness—enabling analysis of error patterns and supporting downstream tasks such as error typology modeling or adaptive feedback generation. Additional gameplay-specific features are available for certain activities (e.g., the number of card flips in memory tasks), providing further behavioral signals used in adaptive learning.

We additionally track the number of attempts per exercise, the score obtained at each step, and overall progression metrics. The dataset distinguishes between two operational modes: the ZPDES adaptive mode (default) and a playlist mode used as a baseline in randomized controlled trials.

\subsubsection{\textbf{Anonymization Process}}

The shared dataset is fully anonymized and contains no personally identifiable information. All identifiers (students, teachers, classrooms, sessions) are randomly generated and serve only to link interactions within the same session or group, without enabling re-identification.

Temporal data are normalized by converting all timestamps to a relative reference (T0), preserving durations (e.g., time on task, session length) while preventing inference of geographic location or time zone.

No demographic attributes (e.g., name, gender, socio-economic status) are included. Although student responses are provided, they are predominantly constrained to structured formats (e.g., multiple choice, drag-and-drop, numerical input), which minimizes the risk of unintended personal information leakage.



\subsection{Processing}
The released dataset is derived from the module subset introduced above and starts from raw interaction logs collected in Adaptiv'Math and MIA Seconde. Before release, these logs are harmonized into a shared attempt-level schema so that both sources expose the same core fields for student identity, exercise identity, timestamp, correctness, answer content, duration, and instructional mode.

The release follows the anonymization procedure described above. Student, teacher, classroom, and session identifiers are pseudonymous and serve only to enable relational reconstruction within the dataset. No directly identifying personal information is included. In addition to the original anonymized interaction timestamp, we provide a session-relative timestamp representation that preserves within-session temporal structure while reducing sensitivity.

We release two attempt-level variants of the interaction data. The primary interaction table preserves the largest possible number of interactions and is intended as the main resource for downstream analyses. For the benchmark experiments reported in this paper, we additionally provide a stricter derived variant together with the preprocessing notebook used to construct it, so that filtering decisions remain transparent and reproducible. This filtered version removes students whose retained history consists only of \texttt{adaptive-test} interactions, students whose retained trajectory contains exercise identifiers absent from the released exercise table, and students with fewer than five attempts overall.

\subsection{Description}
The main released interaction file contains 7,239,840 attempt-level interactions from 45,848 students over 7,845 distinct exercises observed in the retained trajectories. Adaptiv'Math contributes 24,358 students and 5,190,104 attempts, while MIA Seconde contributes 21,490 students and 2,049,736 attempts.

Each interaction record corresponds to a single student attempt on an exercise. The dataset stores anonymized identifiers for the student, exercise, module or playlist, and session, together with the interaction timestamp, the student response, correctness, response duration, and contextual information such as the instructional mode. Attempt indices make it possible to reconstruct repeated trials on the same exercise, while session-relative timestamps preserve the internal temporal structure of learning sequences.

The release also includes an exercise metadata table linking each exercise to its activity, objective, and module, together with gameplay type, pedagogical labels, and source platform. In total, the released scope contains 7,938 exercises. To capture the curricular structure underlying the interactions, we additionally provide a simplified activity-level dependency graph describing prerequisite and unlock relations between activities. This graph covers 6 modules, 78 objectives, and 409 activities.

Finally, the filtered interaction variant used for the benchmark experiments reported in this paper contains 6,663,868 attempts from 36,821 students over 7,747 distinct exercises, corresponding to the removal of 9,027 students (19.69\%) and 575,972 attempts (7.96\%).

\subsection{Statistics}
Table~\ref{tab:dataset_statistics} reports the main scale statistics of the released benchmark. The primary release contains 7{,}239{,}840 attempt-level interactions from 45{,}848 students and 7{,}845 observed exercises, while the filtered benchmark variant used in the experiments retains 6{,}663{,}868 interactions from 36{,}821 students and 7{,}747 observed exercises. Although the filtered variant removes 19.69\% of students, it removes only 7.96\% of attempts, indicating that the excluded trajectories are disproportionately short.

\begin{table*}[t]
\centering
\caption{Core statistics for the released maths benchmark. Exercise counts correspond to exercises observed in the interaction logs; the released exercise metadata table contains 7{,}938 exercises in total.}
\label{tab:dataset_statistics}
\small
\setlength{\tabcolsep}{5pt}
\begin{tabular}{lrrrrrr>{\raggedright\arraybackslash}p{3.2cm}}
\toprule
Split & Students & Attempts & Obs. ex. & Mod. & Obj. & Act. & Work modes \\
\midrule
Adaptiv'Math & 24{,}358 & 5{,}190{,}104 & 6{,}299 & 4 & 61 & 291 & adaptive-test, zpdes \\
MIA Seconde & 21{,}490 & 2{,}049{,}736 & 1{,}546 & 2 & 17 & 118 & adaptive-test, playlist, zpdes \\
Combined release & 45{,}848 & 7{,}239{,}840 & 7{,}845 & 6 & 78 & 409 & adaptive-test, playlist, zpdes \\
Filtered benchmark & 36{,}821 & 6{,}663{,}868 & 7{,}747 & 6 & 78 & 409 & adaptive-test, playlist, zpdes \\
\bottomrule
\end{tabular}
\end{table*}

The benchmark is large but highly heterogeneous at both the student and exercise levels. In the main release, students generate 157.9 attempts on average, but the median trajectory length is 69 attempts, with a 95th percentile of 630 and a maximum of 5{,}431 attempts for a single student. At the exercise level, the median exercise receives 417 attempts, while the mean is 922.9 because usage is strongly concentrated on a subset of exercises. Overall correctness in the main release is 78.8\%.

Interaction durations are similarly heavy-tailed. The median logged response duration is 8.0 seconds, with an interquartile range from 3.9 to 18.4 seconds, a 95th percentile of 78.5 seconds, and a small number of extreme outliers. Figure~\ref{fig:dataset_distributions} summarizes these distributions. Across sources, Adaptiv'Math trajectories are longer and more accurate than MIA Seconde trajectories: the median number of attempts per student is 85 in Adaptiv'Math versus 59 in MIA Seconde, and correctness is 81.2\% versus 72.9\%. Median response duration is 7.3 seconds in Adaptiv'Math and 10.2 seconds in MIA Seconde, consistent with the different target populations and instructional settings of the two platforms.

In the filtered benchmark, the median trajectory length increases to 91 attempts and overall correctness to 79.2\%, while the median response duration remains stable at 8.1 seconds. This confirms that the stricter benchmark preprocessing mainly removes short or weakly informative student histories while preserving the broad interaction profile of the full release.

\begin{figure*}[t]
    \centering
    \includegraphics[width=\linewidth]{figures/dataset_distributions.pdf}
    \caption{Descriptive distributions for the released maths benchmark. Left: distribution of attempts per student, showing a long-tailed trajectory-length profile in both sources. Middle: correctness distribution by source, highlighting higher average correctness in Adaptiv'Math than in MIA Seconde. Right: response-duration distribution, reported on a compressed scale because a small number of extreme outliers coexist with a median interaction time of about 8 seconds.}
    \label{fig:dataset_distributions}
\end{figure*}

\subsection{Release}
\begin{itemize}
    \item Licensing
    \item Hosting
\end{itemize}

\subsection{Scope and usefulness}
The release of our dataset addresses the needs of two broad research communities. For the AI for education (AIED) community, it offers a large-scale, high-quality testbed for knowledge tracing and related tasks. A fundamental limitation of most existing benchmarks in this space is that exercises are either not released at all — reducing students' interaction sequences to anonymous item identifiers — or consist of simple, text-based questions with limited cognitive depth. In contrast, our dataset releases the full exercise content, which was designed by cognitive science researchers to be visual, novel, and cognitively rich, targeting genuine knowledge acquisition rather than surface pattern recognition. This makes our exercises substantially more complex and informative than what is typically available, and opens the door to content-aware models that can reason about what a student is actually being asked, rather than treating exercises as opaque tokens. Overall, our dataset allows questioning the limitations of classic KT models that ignore exercise content, and fosters the development of models able to leverage such rich and complex exercises.

This shift toward content-aware modeling is further supported by the richness of the pedagogical metadata we release. Exercises are explicitly mapped to learning objectives and modules, which are themselves described through detailed natural language descriptions. This multi-level structure — from raw exercise content, to structured objective mappings, to free-text pedagogical descriptions — provides an unusually dense semantic scaffold that models can leverage to generalize across exercises, infer latent knowledge components, or ground predictions in interpretable educational concepts. Such metadata is rarely available in existing datasets, and almost never at this level of granularity. This also permits studying how existing and future KT models can leverage such pedagogical information.

Beyond KT, we believe that the richness of our exercises along with their pedagogical descriptions, combined with a large-scale history of student curricula, will further advance the broader research topic of student modeling. Indeed, beyond KT, modeling students' future answers, misconceptions, or acquired concepts is a key challenge for the field.

Besides AIED, the release of our dataset also closely aligns with the recent and growing interest in applying LLMs to educational purposes \cite{worden_foundationalassist_2026}. The complexity and visual nature of the exercises make them particularly challenging for LLMs, and Visual Language Models (VLMs) appear as natural candidates to describe such exercises or even directly model student behavior. While existing LLMs already seem to struggle at performing KT in zero-shot settings \cite{worden_foundationalassist_2026}, it remains an open question how effectively LLMs or VLMs can be leveraged on our dataset. Beyond student modeling, the detailed feedback provided to students after each exercise constitutes a rare source of grounded, domain-specific instructional text that can be used to study, train, or evaluate LLMs acting as tutors or educational assistants.

\section{Evaluating existing approaches on \DATASET}
Evaluations to run:
\begin{itemize}
    \item Question the limit of classic KT models: evaluate PyKT approaches and LLMs or VLMs
    \item About pedagogical information, create a graph and evaluate LLMs/VLMs and graph-based KT approaches??
    \item About modeling more generally students: study if LLMs or VLMs are able to predict the students' answer (as in Foundational assist, study on multi-choice questions?)
    \item 
\end{itemize}


\subsection{Evaluation protocol}
\begin{itemize}
    \item Knowledge Components creation
    \item Train/test split
    \item Sequences and padding
    \item Baselines (PyKT with HP tuning and LLMs)
    \item AUC and accuracy reported
\end{itemize}



\subsection{Discussion}

\small
\bibliography{references}

\newpage
\input{checklist.tex}

\end{document}