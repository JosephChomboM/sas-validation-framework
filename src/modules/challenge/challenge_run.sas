/* =========================================================================
challenge_run.sas - Orquestador final de Challenge (METOD9)
Consolida resultados por algoritmo y selecciona el champion final.
========================================================================= */
%include "&fw_root./src/modules/challenge/impl/challenge_champion.sas";
%include "&fw_root./src/modules/challenge/impl/challenge_report.sas";

%macro challenge_run(input_caslib=PROC, train_table=_train_input,
    oot_table=_oot_input, output_caslib=OUT, troncal_id=, scope=, run_id=);

    %local _chall_model_type _scope_abbr _chall_mode_eff _chall_seg_num
        _chall_segment_lbl _chall_report_path _chall_images_path
        _chall_tables_path _chall_dir_rc _chall_report_prefix
        _chall_tbl_prefix _prefix_root _algo_codes _i _algo_code
        _algo_prefix _has_registry _registry_sets _first_algo_prefix
        _champ_artifact_prefix _model_low _model_high _monthly_exists
        _bmk_exists;

    %put NOTE:======================================================;
    %put NOTE: [challenge_run] INICIO - troncal=&troncal_id. scope=&scope.;
    %put NOTE:======================================================;

    proc sql noprint;
        select strip(model_type)
          into :_chall_model_type trimmed
        from casuser.cfg_troncales
        where troncal_id=&troncal_id.;
    quit;

    %if %length(%superq(_chall_model_type))=0 %then %let _chall_model_type=BHV;
    %let _chall_mode_eff=%upcase(%superq(challenge_mode));
    %if %length(%superq(_chall_mode_eff))=0 %then %let _chall_mode_eff=AUTO;

    %if %substr(&scope., 1, 3)=seg %then %do;
        %let _chall_seg_num=%sysfunc(inputn(%substr(&scope., 4), best.));
        %let _chall_segment_lbl=&scope.;
        %let _scope_abbr=&scope.;
    %end;
    %else %do;
        %let _chall_seg_num=.;
        %let _chall_segment_lbl=base;
        %let _scope_abbr=base;
    %end;

    %if %upcase(&_chall_model_type.)=BHV %then %do;
        %let _model_low=0.50;
        %let _model_high=0.60;
    %end;
    %else %do;
        %let _model_low=0.40;
        %let _model_high=0.50;
    %end;

    %let _prefix_root=;
    %if &_chall_mode_eff.=CUSTOM %then %do;
        %let _chall_report_path=&fw_root./outputs/runs/&run_id./experiments;
        %let _chall_images_path=&fw_root./outputs/runs/&run_id./experiments;
        %let _chall_tables_path=&fw_root./outputs/runs/&run_id./experiments;
        %let _chall_report_prefix=custom_challenge_troncal_&troncal_id._&_scope_abbr.;
        %let _chall_tbl_prefix=cx_chall_t&troncal_id._&_scope_abbr.;
        %let _prefix_root=cx_;
    %end;
    %else %do;
        %let _chall_report_path=&fw_root./outputs/runs/&run_id./reports/METOD9;
        %let _chall_images_path=&fw_root./outputs/runs/&run_id./images/METOD9;
        %let _chall_tables_path=&fw_root./outputs/runs/&run_id./tables/METOD9;
        %let _chall_report_prefix=challenge_troncal_&troncal_id._&_scope_abbr.;
        %let _chall_tbl_prefix=chall_t&troncal_id._&_scope_abbr.;
        %let _chall_dir_rc=%sysfunc(dcreate(METOD9,
            &fw_root./outputs/runs/&run_id./reports));
        %let _chall_dir_rc=%sysfunc(dcreate(METOD9,
            &fw_root./outputs/runs/&run_id./images));
        %let _chall_dir_rc=%sysfunc(dcreate(METOD9,
            &fw_root./outputs/runs/&run_id./tables));
    %end;

    libname _challin "&_chall_tables_path.";

    %let _algo_codes=gb dt rf;
    %let _registry_sets=;
    %let _first_algo_prefix=;

    %do _i=1 %to %sysfunc(countw(&_algo_codes., %str( )));
        %let _algo_code=%scan(&_algo_codes., &_i., %str( ));
        %let _algo_prefix=&_prefix_root.&_algo_code._t&troncal_id._&_scope_abbr.;
        %let _has_registry=0;

        proc sql noprint;
            select count(*) into :_has_registry trimmed
            from dictionary.tables
            where upcase(libname)='_CHALLIN'
              and upcase(memname)=upcase("&_algo_prefix._rgst");
        quit;

        %if &_has_registry. > 0 %then %do;
            %if %length(%superq(_registry_sets))=0 %then
                %let _registry_sets=_challin.&_algo_prefix._rgst;
            %else
                %let _registry_sets=&_registry_sets. _challin.&_algo_prefix._rgst;

            %if %length(%superq(_first_algo_prefix))=0 %then
                %let _first_algo_prefix=&_algo_prefix.;

            %put NOTE: [challenge_run] Registry encontrado para &_algo_code.
                prefijo=&_algo_prefix.;
        %end;
    %end;

    %if %length(%superq(_registry_sets))=0 %then %do;
        %put WARNING: [challenge_run] No se encontraron registries de algoritmos
            para troncal=&troncal_id. scope=&scope. mode=&_chall_mode_eff..;
        libname _challin clear;
        %return;
    %end;

    data work._chall_registry;
        set &_registry_sets.;
    run;

    proc sort data=work._chall_registry;
        by descending Gini_Penalizado descending Gini_OOT descending Gini_Train;
    run;

    data work._chall_algo_champions;
        set work._chall_registry(where=(Is_Champion=1));
    run;

    %let _bmk_exists=0;
    proc sql noprint;
        select count(*) into :_bmk_exists trimmed
        from dictionary.tables
        where upcase(libname)='_CHALLIN'
          and upcase(memname)=upcase("&_first_algo_prefix._bmk");
    quit;

    %if &_bmk_exists.=0 %then %do;
        %put ERROR: [challenge_run] No se encontro benchmark base para
            prefijo=&_first_algo_prefix..;
        libname _challin clear;
        %return;
    %end;

    data work._chall_benchmark_global;
        set _challin.&_first_algo_prefix._bmk;
    run;

    %_chall_build_champion_summary(registry=work._chall_algo_champions,
        benchmark=work._chall_benchmark_global,
        out=work._chall_champion_summary);

    proc sql noprint;
        select strip(Artifact_Prefix) into :_champ_artifact_prefix trimmed
        from work._chall_champion_summary;
    quit;

    %if %length(%superq(_champ_artifact_prefix)) > 0 %then %do;
        data work._chall_registry;
            set work._chall_registry;
            Is_Global_Champion=(Is_Champion=1 and
                upcase(Artifact_Prefix)=upcase("&_champ_artifact_prefix."));
        run;
    %end;
    %else %do;
        data work._chall_registry;
            set work._chall_registry;
            Is_Global_Champion=0;
        run;
    %end;

    %let _monthly_exists=0;
    %if %length(%superq(_champ_artifact_prefix)) > 0 %then %do;
        proc sql noprint;
            select count(*) into :_monthly_exists trimmed
            from dictionary.tables
            where upcase(libname)='_CHALLIN'
              and upcase(memname)=upcase("&_champ_artifact_prefix._mnly");
        quit;
    %end;

    %if &_monthly_exists. > 0 %then %do;
        data work._chall_monthly_compare;
            set _challin.&_champ_artifact_prefix._mnly;
        run;
    %end;
    %else %do;
        data work._chall_monthly_compare;
            length Periodo 8 N_Total 8 N_Default 8 Gini 8 Model_Rank 8
                Model_Label $64 Algo_Name $32 Source $16;
            stop;
        run;
    %end;

    libname _chalout "&_chall_tables_path.";

    data _chalout.&_chall_tbl_prefix._rgst;
        set work._chall_registry;
    run;
    data _chalout.&_chall_tbl_prefix._algc;
        set work._chall_algo_champions;
    run;
    data _chalout.&_chall_tbl_prefix._bmk;
        set work._chall_benchmark_global;
    run;
    data _chalout.&_chall_tbl_prefix._chmp;
        set work._chall_champion_summary;
    run;
    data _chalout.&_chall_tbl_prefix._mnly;
        set work._chall_monthly_compare;
    run;

    libname _chalout clear;

    %_challenge_report(registry_data=work._chall_registry,
        champion_data=work._chall_champion_summary,
        monthly_data=work._chall_monthly_compare,
        report_path=&_chall_report_path., images_path=&_chall_images_path.,
        file_prefix=&_chall_report_prefix., model_low=&_model_low.,
        model_high=&_model_high.);

    proc datasets library=work nolist nowarn;
        delete _chall_registry _chall_algo_champions _chall_benchmark_global
            _chall_champion_summary _chall_monthly_compare;
    quit;

    libname _challin clear;

    %put NOTE:======================================================;
    %put NOTE: [challenge_run] FIN - &_chall_report_prefix.;
    %put NOTE:======================================================;
%mend challenge_run;
