/* =========================================================================
challenge_report.sas - Reportes ODS para METOD9 Challenge
========================================================================= */

%macro _chall_report_style(col=, fmt=);
    compute &col.;
        if not missing(strip(put(&col., &fmt..))) then
            call define(_col_, "style",
                cats("style=[backgroundcolor=", strip(put(&col., &fmt..)),
                "]"));
    endcomp;
%mend _chall_report_style;

%macro _challenge_report(benchmark_data=, eval_data=, topk_data=, topn_data=,
    monthly_data=, registry_data=, champion_data=, report_path=, images_path=,
    file_prefix=);

    proc format;
        value GainFmt
            low-<0='lightred'
            0-high='lightgreen';
        value ChampFmt
            0='white'
            1='lightgreen';
    run;

    ods graphics on;
    ods listing gpath="&images_path.";
    ods html5 file="&report_path./&file_prefix..html"
        options(bitmap_mode="inline");
    ods excel file="&report_path./&file_prefix..xlsx"
        options(sheet_name="BENCHMARK" sheet_interval="none"
        embedded_titles="yes" frozen_headers="yes" autofilter="all");

    title "Challenge - Benchmark Base";
    proc report data=&benchmark_data. nowd missing;
        columns Dataset Score_Source Gini Gini_Penalizado;
        define Dataset / display "Dataset";
        define Score_Source / display "Score Base";
        define Gini / display "GINI" format=8.4;
        define Gini_Penalizado / display "GINI Penalizado" format=8.4;
    run;
    title;

    ods excel options(sheet_name="AUTOTUNE" sheet_interval="now");
    title "Challenge - Evaluation History";
    proc report data=&eval_data. nowd missing;
        columns Iteration GiniCoefficient NTREE M LEARNINGRATE SUBSAMPLERATE
            LASSO RIDGE NBINS MAXLEVEL LEAFSIZE;
        define Iteration / display "Iter";
        define GiniCoefficient / display "GINI Tune" format=8.4;
        define NTREE / display "NTrees";
        define M / display "Vars To Try";
        define LEARNINGRATE / display "Learning Rate";
        define SUBSAMPLERATE / display "Sampling Rate";
        define LASSO / display "Lasso";
        define RIDGE / display "Ridge";
        define NBINS / display "NBins";
        define MAXLEVEL / display "MaxDepth";
        define LEAFSIZE / display "LeafSize";
    run;
    title;

    ods excel options(sheet_name="TOPK" sheet_interval="now");
    title "Challenge - Ranking Top K";
    proc report data=&topk_data. nowd missing;
        columns Model_Rank Gini_Train Gini_OOT Gini_Penalizado NTREE M
            LEARNINGRATE SUBSAMPLERATE LASSO RIDGE NBINS MAXLEVEL LEAFSIZE;
        define Model_Rank / display "Rank";
        define Gini_Train / display "GINI Train" format=8.4;
        define Gini_OOT / display "GINI OOT" format=8.4;
        define Gini_Penalizado / display "GINI Penalizado" format=8.4;
        define NTREE / display "NTrees";
        define M / display "Vars To Try";
        define LEARNINGRATE / display "Learning Rate";
        define SUBSAMPLERATE / display "Sampling Rate";
        define LASSO / display "Lasso";
        define RIDGE / display "Ridge";
        define NBINS / display "NBins";
        define MAXLEVEL / display "MaxDepth";
        define LEAFSIZE / display "LeafSize";
    run;
    title;

    ods excel options(sheet_name="TOPN" sheet_interval="now");
    title "Challenge - Ranking Final";
    proc report data=&topn_data. nowd missing;
        columns Model_Rank Gini_Train Gini_OOT Gini_Penalizado NTREE M
            LEARNINGRATE SUBSAMPLERATE LASSO RIDGE NBINS MAXLEVEL LEAFSIZE
            Is_Champion;
        define Model_Rank / display "Rank";
        define Gini_Train / display "GINI Train" format=8.4;
        define Gini_OOT / display "GINI OOT" format=8.4;
        define Gini_Penalizado / display "GINI Penalizado" format=8.4;
        define NTREE / display "NTrees";
        define M / display "Vars To Try";
        define LEARNINGRATE / display "Learning Rate";
        define SUBSAMPLERATE / display "Sampling Rate";
        define LASSO / display "Lasso";
        define RIDGE / display "Ridge";
        define NBINS / display "NBins";
        define MAXLEVEL / display "MaxDepth";
        define LEAFSIZE / display "LeafSize";
        define Is_Champion / display "Champion";
        %_chall_report_style(col=Is_Champion, fmt=ChampFmt)
    run;
    title;

    ods excel options(sheet_name="MONTHLY" sheet_interval="now");
    title "Challenge - Comparativo Mensual";
    proc report data=&monthly_data. nowd missing;
        columns Periodo Model_Label Source N_Total N_Default Gini;
        define Periodo / display "Periodo";
        define Model_Label / display "Modelo";
        define Source / display "Fuente";
        define N_Total / display "N Total";
        define N_Default / display "N Default";
        define Gini / display "GINI" format=8.4;
    run;
    title;

    ods excel options(sheet_name="REGISTRY" sheet_interval="now");
    title "Challenge - Registry";
    proc report data=&registry_data. nowd missing;
        columns Algo_Name Troncal_ID Scope Seg_ID Segment_Label Model_Rank
            Gini_Train Gini_OOT Gini_Penalizado Astore_Name Is_Champion;
        define Algo_Name / display "Algoritmo";
        define Troncal_ID / display "Troncal";
        define Scope / display "Scope";
        define Seg_ID / display "Seg ID";
        define Segment_Label / display "Segment";
        define Model_Rank / display "Rank";
        define Gini_Train / display "GINI Train" format=8.4;
        define Gini_OOT / display "GINI OOT" format=8.4;
        define Gini_Penalizado / display "GINI Penalizado" format=8.4;
        define Astore_Name / display "ASTORE";
        define Is_Champion / display "Champion";
        %_chall_report_style(col=Is_Champion, fmt=ChampFmt)
    run;
    title;

    ods excel options(sheet_name="CHAMPION" sheet_interval="now");
    title "Challenge - Champion Summary";
    proc report data=&champion_data. nowd missing;
        columns Algo_Name Scope Segment_Label Model_Rank Gini_Train Gini_OOT
            Gini_Penalizado Benchmark_Gini_Train Benchmark_Gini_OOT
            Benchmark_Gini_Penalizado Improvement_Train Improvement_OOT
            Improvement_Penalizado Astore_Name;
        define Algo_Name / display "Algoritmo";
        define Scope / display "Scope";
        define Segment_Label / display "Segment";
        define Model_Rank / display "Rank";
        define Gini_Train / display "GINI Train" format=8.4;
        define Gini_OOT / display "GINI OOT" format=8.4;
        define Gini_Penalizado / display "GINI Penalizado" format=8.4;
        define Benchmark_Gini_Train / display "Benchmark Train" format=8.4;
        define Benchmark_Gini_OOT / display "Benchmark OOT" format=8.4;
        define Benchmark_Gini_Penalizado / display "Benchmark Penal." format=8.4;
        define Improvement_Train / display "Mejora Train" format=8.4;
        define Improvement_OOT / display "Mejora OOT" format=8.4;
        define Improvement_Penalizado / display "Mejora Penal." format=8.4;
        define Astore_Name / display "ASTORE";
        %_chall_report_style(col=Improvement_Train, fmt=GainFmt)
        %_chall_report_style(col=Improvement_OOT, fmt=GainFmt)
        %_chall_report_style(col=Improvement_Penalizado, fmt=GainFmt)
    run;
    title;

    ods excel options(sheet_name="PLOTS" sheet_interval="now");

    ods graphics / imagename="&file_prefix._monthly" imagefmt=jpeg;
    proc sgplot data=&monthly_data.;
        title "Challenge - Benchmark vs Top Models";
        series x=Periodo y=Gini / group=Model_Label markers;
        yaxis label="GINI" min=0 max=1;
        xaxis label="Periodo";
        keylegend / position=bottom;
    run;
    title;
    ods graphics / reset=all;

    ods graphics / imagename="&file_prefix._topn" imagefmt=jpeg;
    proc sgplot data=&topn_data.;
        title "Challenge - GINI Penalizado Final";
        vbarparm category=Model_Rank response=Gini_Penalizado /
            datalabel group=Is_Champion;
        yaxis label="GINI Penalizado";
        xaxis label="Rank Final";
    run;
    title;
    ods graphics / reset=all;

    ods graphics / imagename="&file_prefix._tune" imagefmt=jpeg;
    proc sgplot data=&eval_data.;
        title "Challenge - Autotune History";
        series x=Iteration y=GiniCoefficient / markers;
        yaxis label="GINI";
        xaxis label="Iteracion";
    run;
    title;
    ods graphics / reset=all;

    ods excel close;
    ods html5 close;
    ods graphics / reset=all;
    ods graphics off;
%mend _challenge_report;
