/* =========================================================================
challenge_report.sas - Reporte consolidado multi-algoritmo para METOD9
========================================================================= */

%macro _chall_report_style(col=, fmt=);
    compute &col.;
        if not missing(strip(put(&col., &fmt..))) then
            call define(_col_, "style",
                cats("style=[backgroundcolor=", strip(put(&col., &fmt..)),
                "]"));
    endcomp;
%mend _chall_report_style;

%macro _challenge_report(registry_data=work._chall_registry,
    champion_data=work._chall_selected_models,
    global_data=work._chall_champion_summary,
    monthly_data=work._chall_monthly_compare, report_path=, images_path=,
    file_prefix=, model_low=0.4, model_high=0.5, troncal_id=,
    data_type=);

    %local _monthly_exists _monthly_n _global_exists _global_n _plot_n
        _plot_title;
    %let _monthly_exists=0;
    %let _monthly_n=0;
    %let _global_exists=0;
    %let _global_n=0;
    %let _plot_n=0;
    %let _plot_title=Gini Global Challenge;
    %if %length(%superq(troncal_id)) > 0 %then
        %let _plot_title=&_plot_title. - Troncal &troncal_id.;
    %if %length(%superq(data_type)) > 0 %then
        %let _plot_title=&_plot_title. - &data_type.;

    %if %sysfunc(exist(&monthly_data.)) %then %do;
        %let _monthly_exists=1;
        proc sql noprint;
            select count(*) into :_monthly_n trimmed
            from &monthly_data.;
        quit;
    %end;

    %if %sysfunc(exist(&global_data.)) %then %do;
        %let _global_exists=1;
        proc sql noprint;
            select count(*) into :_global_n trimmed
            from &global_data.;
        quit;
    %end;

    proc format;
        value GainFmt
            low-<0='cxF4CCCC'
            0-high='cxD9EAD3';
        value ChampFmt
            0='white'
            1='cxD9EAD3';
    run;

    ods graphics on;
    ods listing gpath="&images_path.";
    ods html5 file="&report_path./&file_prefix..html"
        options(bitmap_mode="inline");
    ods excel file="&report_path./&file_prefix..xlsx"
        options(sheet_name="REGISTRY" sheet_interval="none"
        embedded_titles="yes" frozen_headers="yes" autofilter="all");

    title "Challenge - Registry Consolidado";
    proc report data=&registry_data. nowd missing;
        columns Algo_Name Algo_Code Troncal_ID Scope Seg_ID Segment_Label
            Model_Rank Gini_Train Gini_OOT Gini_Penalizado
            Artifact_Prefix Astore_Name Is_Champion;
        define Algo_Name / display "Algoritmo";
        define Algo_Code / display "Cod";
        define Troncal_ID / display "Troncal";
        define Scope / display "Scope";
        define Seg_ID / display "Seg ID";
        define Segment_Label / display "Segment";
        define Model_Rank / display "Rank";
        define Gini_Train / display "GINI Train" format=8.4;
        define Gini_OOT / display "GINI OOT" format=8.4;
        define Gini_Penalizado / display "GINI Penalizado" format=8.4;
        define Artifact_Prefix / display "Artifact Prefix";
        define Astore_Name / display "ASTORE";
        define Is_Champion / display "Champion Local";
        %_chall_report_style(col=Is_Champion, fmt=ChampFmt)
    run;
    title;

    ods excel options(sheet_name="SELECTED_MODELS" sheet_interval="now");
    title "Challenge - Modelos Seleccionados";
    proc report data=&champion_data. nowd missing;
        columns Algo_Name Scope Seg_ID Segment_Label Var_Seg Model_Rank
            Gini_Train Gini_OOT Gini_Penalizado Astore_Name Models_Path;
        define Algo_Name / display "Algoritmo";
        define Scope / display "Scope";
        define Seg_ID / display "Seg ID";
        define Segment_Label / display "Segment";
        define Var_Seg / display "Var Seg";
        define Model_Rank / display "Rank";
        define Gini_Train / display "GINI Train" format=8.4;
        define Gini_OOT / display "GINI OOT" format=8.4;
        define Gini_Penalizado / display "GINI Penalizado" format=8.4;
        define Astore_Name / display "ASTORE";
        define Models_Path / display "Ruta Modelo" width=80 flow;
    run;
    title;

    %if &_global_exists.=1 and &_global_n. > 0 %then %do;
        ods excel options(sheet_name="GLOBAL" sheet_interval="now");
        title "Challenge - Resumen Global";
        proc report data=&global_data. nowd missing;
            columns Scope Champion_Mode N_Selected_Models Gini_Train Gini_OOT
                Gini_Penalizado Benchmark_Gini_Train Benchmark_Gini_OOT
                Benchmark_Gini_Penalizado Improvement_Train Improvement_OOT
                Improvement_Penalizado;
            define Scope / display "Scope";
            define Champion_Mode / display "Champion Mode";
            define N_Selected_Models / display "N Modelos";
            define Gini_Train / display "GINI Train" format=8.4;
            define Gini_OOT / display "GINI OOT" format=8.4;
            define Gini_Penalizado / display "GINI Penalizado" format=8.4;
            define Benchmark_Gini_Train / display "Benchmark Train" format=8.4;
            define Benchmark_Gini_OOT / display "Benchmark OOT" format=8.4;
            define Benchmark_Gini_Penalizado / display "Benchmark Penal." format=8.4;
            define Improvement_Train / display "Mejora Train" format=8.4;
            define Improvement_OOT / display "Mejora OOT" format=8.4;
            define Improvement_Penalizado / display "Mejora Penal." format=8.4;
            %_chall_report_style(col=Improvement_Train, fmt=GainFmt)
            %_chall_report_style(col=Improvement_OOT, fmt=GainFmt)
            %_chall_report_style(col=Improvement_Penalizado, fmt=GainFmt)
        run;
        title;
    %end;

    %if &_monthly_exists.=1 and &_monthly_n. > 0 %then %do;
        ods excel options(sheet_name="MONTHLY" sheet_interval="now");
        title "Challenge - Benchmark vs Champion";
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

        ods excel options(sheet_name="PLOTS" sheet_interval="now");

        data work._chall_plot_monthly;
            set &monthly_data.;
            where upcase(Source)='CHAMPION';
        run;

        proc sql noprint;
            select count(*) into :_plot_n trimmed
            from work._chall_plot_monthly;
        quit;

        %if &_plot_n. = 0 %then %do;
            data work._chall_plot_monthly;
                set &monthly_data.;
            run;
        %end;

        proc sort data=work._chall_plot_monthly;
            by Periodo;
        run;

        ods graphics / imagename="&file_prefix._monthly" imagefmt=jpeg;
        proc sgplot data=work._chall_plot_monthly;
            title "&_plot_title.";
            vbar Periodo / response=N_Total transparency=0.7 barwidth=0.5
                name='bar' fillattrs=(color=gray)
                legendlabel='Cuentas' datalabelfitpolicy=rotate;
            vline Periodo / response=Gini markers
                markerattrs=(symbol=circlefilled color=black size=10px)
                lineattrs=(thickness=0 color=black)
                name='line' legendlabel='Gini' y2axis;
            refline &model_low. / axis=y2
                lineattrs=(color=orange pattern=2 thickness=2)
                labelloc=inside labelattrs=(color=orange)
                name='acep' legendlabel='Aceptable';
            refline &model_high. / axis=y2
                lineattrs=(color=limegreen pattern=2 thickness=2)
                labelloc=inside labelattrs=(color=limegreen)
                name='sat' legendlabel='Satisfactorio';
            yaxis grid label="Cuentas" min=0 offsetmin=0;
            y2axis grid label="Gini" min=0 max=1;
            xaxis display=all label="Periodo";
            keylegend 'bar' 'line' 'acep' 'sat' / position=bottom noborder;
        run;
        title;
        ods graphics / reset=all;

        proc datasets library=work nolist nowarn;
            delete _chall_plot_monthly;
        quit;
    %end;

    ods excel close;
    ods html5 close;
    ods graphics / reset=all;
    ods graphics off;
%mend _challenge_report;
