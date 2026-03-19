/* =========================================================================
gradient_boosting_run.sas - Macro publica del modulo Gradient Boosting
========================================================================= */
%include "&fw_root./src/modules/gradient_boosting/gradient_boosting_contract.sas";
%include "&fw_root./src/modules/challenge/impl/challenge_prechallenge.sas";
%include "&fw_root./src/modules/gradient_boosting/impl/gradient_boosting_compute.sas";
%include "&fw_root./src/modules/gradient_boosting/impl/gradient_boosting_parallel.sas";
%include "&fw_root./src/modules/challenge/impl/challenge_registry.sas";
%include "&fw_root./src/modules/challenge/impl/challenge_champion.sas";
%include "&fw_root./src/modules/gradient_boosting/impl/gradient_boosting_report.sas";

%macro gradient_boosting_run(input_caslib=PROC, train_table=_train_input,
    oot_table=_oot_input, output_caslib=OUT, troncal_id=, scope=, run_id=);

    %global _chall_rc _chall_target _chall_byvar _chall_vars_num
        _chall_vars_cat _chall_train_table _chall_oot_table
        _chall_penalty_lambda _chall_gb_stagnation _chall_run_root
        _chall_models_subdir _chall_astore_name _chall_fw_root _chall_run_id
        _chall_scope _chall_troncal_id _chall_seg_id _chall_segment_label
        _chall_var_seg_name _chall_log_tag;

    %local _chall_target_cfg _chall_pd _chall_xb _chall_score _chall_byvar_cfg
        _chall_id_cfg _chall_var_seg_cfg _chall_model_type _chall_scope_abbr
        _chall_report_path _chall_images_path _chall_tables_path
        _chall_models_path _chall_file_prefix _chall_tbl_prefix _chall_dir_rc
        _chall_vars_num_cfg _chall_vars_cat_cfg _chall_keep_train
        _chall_keep_oot
        _chall_mode_eff _chall_score_mode _chall_top_k _chall_top_models
        _chall_partition_pct _chall_seed _chall_presample_mode
        _chall_presample_cells _chall_sampling_ratio _chall_sampled_flag
        _chall_total_cells _chall_seg_num _chall_id_final _chall_seg_final
        _chall_report_prefix _chall_models_file _chall_segment_lbl
        _chall_custom_mode _chall_nparallel _chall_ntrees_cap;

    %let _chall_rc=0;
    %let _chall_target=;
    %let _chall_byvar=;
    %let _chall_vars_num=;
    %let _chall_vars_cat=;
    %let _chall_train_table=_chall_train_base;
    %let _chall_oot_table=_chall_oot_base;

    %put NOTE:======================================================;
    %put NOTE: [gradient_boosting_run] INICIO - troncal=&troncal_id.
        scope=&scope.;
    %put NOTE:======================================================;

    proc sql noprint;
        select strip(target), strip(pd), strip(xb), strip(byvar),
               strip(id_var_id), strip(var_seg), strip(model_type)
          into :_chall_target_cfg trimmed,
               :_chall_pd trimmed,
               :_chall_xb trimmed,
               :_chall_byvar_cfg trimmed,
               :_chall_id_cfg trimmed,
               :_chall_var_seg_cfg trimmed,
               :_chall_model_type trimmed
        from casuser.cfg_troncales
        where troncal_id=&troncal_id.;
    quit;

    %let _chall_score_mode=%upcase(%superq(gb_score_source));
    %if %length(%superq(_chall_score_mode))=0 %then %let _chall_score_mode=AUTO;
    %let _chall_mode_eff=%upcase(%superq(gb_mode));
    %if %length(%superq(_chall_mode_eff))=0 %then %let _chall_mode_eff=AUTO;
    %let _chall_custom_mode=0;

    %if &_chall_mode_eff.=CUSTOM %then %do;
        %if %length(%superq(gb_custom_vars_num)) > 0 or
            %length(%superq(gb_custom_vars_cat)) > 0 %then %do;
            %let _chall_vars_num_cfg=&gb_custom_vars_num.;
            %let _chall_vars_cat_cfg=&gb_custom_vars_cat.;
            %let _chall_custom_mode=1;
            %if %length(%superq(gb_custom_target)) > 0 %then
                %let _chall_target_cfg=&gb_custom_target.;
            %put NOTE: [gradient_boosting_run] Modo CUSTOM activado.;
        %end;
        %else %do;
            %put WARNING: [gradient_boosting_run] gb_mode=CUSTOM sin listas custom.
                Se aplica fallback a AUTO.;
            %let _chall_mode_eff=AUTO;
        %end;
    %end;

    %if %substr(&scope., 1, 3)=seg %then %do;
        %let _chall_seg_num=%sysfunc(inputn(%substr(&scope., 4), best.));
        %let _chall_segment_lbl=&scope.;
    %end;
    %else %do;
        %let _chall_seg_num=.;
        %let _chall_segment_lbl=base;
    %end;

    %if &_chall_custom_mode.=0 %then %do;
        %if %substr(&scope., 1, 3)=seg %then %do;
            proc sql noprint;
                select strip(num_list), strip(cat_list)
                  into :_chall_vars_num_cfg trimmed,
                       :_chall_vars_cat_cfg trimmed
                from casuser.cfg_segmentos
                where troncal_id=&troncal_id.
                  and seg_id=&_chall_seg_num.;
            quit;
        %end;

        %if %length(%superq(_chall_vars_num_cfg))=0 %then %do;
            proc sql noprint;
                select strip(num_unv) into :_chall_vars_num_cfg trimmed
                from casuser.cfg_troncales
                where troncal_id=&troncal_id.;
            quit;
        %end;
        %if %length(%superq(_chall_vars_cat_cfg))=0 %then %do;
            proc sql noprint;
                select strip(cat_unv) into :_chall_vars_cat_cfg trimmed
                from casuser.cfg_troncales
                where troncal_id=&troncal_id.;
            quit;
        %end;
    %end;

    %let _chall_score=;
    %if &_chall_score_mode.=PD %then %let _chall_score=&_chall_pd.;
    %else %if &_chall_score_mode.=XB %then %let _chall_score=&_chall_xb.;
    %else %if &_chall_score_mode.=CUSTOM %then %do;
        %if %length(%superq(gb_custom_score_var)) > 0 %then
            %let _chall_score=&gb_custom_score_var.;
    %end;
    %if %length(%superq(_chall_score))=0 %then %do;
        %if %length(%superq(_chall_pd)) > 0 %then %let _chall_score=&_chall_pd.;
        %else %let _chall_score=&_chall_xb.;
    %end;

    %_chall_intersect_inputs(train_data=&input_caslib..&train_table.,
        oot_data=&input_caslib..&oot_table., vars_num=&_chall_vars_num_cfg.,
        vars_cat=&_chall_vars_cat_cfg., id_var=&_chall_id_cfg.,
        var_seg=&_chall_var_seg_cfg., out_num=_chall_vars_num,
        out_cat=_chall_vars_cat, out_id_var=_chall_id_final,
        out_seg_var=_chall_seg_final);

    %let _chall_target=&_chall_target_cfg.;
    %let _chall_byvar=&_chall_byvar_cfg.;

    %gradient_boosting_contract(input_caslib=&input_caslib.,
        train_table=&train_table.,
        oot_table=&oot_table., target=&_chall_target., score_var=&_chall_score.,
        byvar=&_chall_byvar., id_var=&_chall_id_final.,
        vars_num=&_chall_vars_num., vars_cat=&_chall_vars_cat.,
        var_seg=&_chall_seg_final.);

    %if &_chall_rc. ne 0 %then %do;
        %put ERROR: [gradient_boosting_run] Contract fallido.
            Gradient Boosting abortado.;
        %return;
    %end;

    %if %substr(&scope., 1, 3)=seg %then %let _chall_scope_abbr=&scope.;
    %else %let _chall_scope_abbr=base;

    %let _chall_report_prefix=gradient_boosting_troncal_&troncal_id._&_chall_scope_abbr.;
    %let _chall_tbl_prefix=gb_t&troncal_id._&_chall_scope_abbr.;

    %if &_chall_custom_mode.=1 %then %do;
        %let _chall_report_path=&fw_root./outputs/runs/&run_id./experiments;
        %let _chall_images_path=&fw_root./outputs/runs/&run_id./experiments;
        %let _chall_tables_path=&fw_root./outputs/runs/&run_id./experiments;
        %let _chall_report_prefix=custom_&_chall_report_prefix.;
        %let _chall_tbl_prefix=cx_&_chall_tbl_prefix.;
    %end;
    %else %do;
        %let _chall_report_path=&fw_root./outputs/runs/&run_id./reports/METOD9;
        %let _chall_images_path=&fw_root./outputs/runs/&run_id./images/METOD9;
        %let _chall_tables_path=&fw_root./outputs/runs/&run_id./tables/METOD9;
        %let _chall_dir_rc=%sysfunc(dcreate(METOD9,
            &fw_root./outputs/runs/&run_id./reports));
        %let _chall_dir_rc=%sysfunc(dcreate(METOD9,
            &fw_root./outputs/runs/&run_id./images));
        %let _chall_dir_rc=%sysfunc(dcreate(METOD9,
            &fw_root./outputs/runs/&run_id./tables));
    %end;

    %let _chall_models_path=&fw_root./outputs/runs/&run_id./models/METOD9;
    %let _chall_dir_rc=%sysfunc(dcreate(METOD9,
        &fw_root./outputs/runs/&run_id./models));
    %let _chall_astore_name=&_chall_tbl_prefix._gb_chmp;
    %let _chall_log_tag=&_chall_tbl_prefix.;

    %let _chall_keep_train=;
    %_chall_push_unique(list_name=_chall_keep_train, value=&_chall_target.);
    %_chall_push_unique(list_name=_chall_keep_train, value=&_chall_score.);
    %_chall_push_unique(list_name=_chall_keep_train, value=&_chall_byvar.);
    %if %length(%superq(_chall_id_final)) > 0 %then
        %_chall_push_unique(list_name=_chall_keep_train, value=&_chall_id_final.);
    %if %length(%superq(_chall_seg_final)) > 0 %then
        %_chall_push_unique(list_name=_chall_keep_train, value=&_chall_seg_final.);

    %local _keep_idx _keep_var;
    %let _keep_idx=1;
    %let _keep_var=%scan(%superq(_chall_vars_num), &_keep_idx., %str( ));
    %do %while(%length(%superq(_keep_var)) > 0);
        %_chall_push_unique(list_name=_chall_keep_train, value=&_keep_var.);
        %let _keep_idx=%eval(&_keep_idx. + 1);
        %let _keep_var=%scan(%superq(_chall_vars_num), &_keep_idx., %str( ));
    %end;

    %let _keep_idx=1;
    %let _keep_var=%scan(%superq(_chall_vars_cat), &_keep_idx., %str( ));
    %do %while(%length(%superq(_keep_var)) > 0);
        %_chall_push_unique(list_name=_chall_keep_train, value=&_keep_var.);
        %let _keep_idx=%eval(&_keep_idx. + 1);
        %let _keep_var=%scan(%superq(_chall_vars_cat), &_keep_idx., %str( ));
    %end;

    %let _chall_keep_oot=&_chall_keep_train.;

    %_chall_prepare_inputs(input_caslib=&input_caslib., train_table=&train_table.,
        oot_table=&oot_table., keep_train=&_chall_keep_train.,
        keep_oot=&_chall_keep_oot., out_train=casuser._chall_train_src,
        out_oot=casuser._chall_oot_src);

    data work._chall_train_raw;
        set casuser._chall_train_src;
    run;

    data work._chall_oot_raw;
        set casuser._chall_oot_src;
    run;

    %let _chall_presample_mode=%upcase(%superq(gb_presample_enabled));
    %if %length(%superq(_chall_presample_mode))=0 %then
        %let _chall_presample_mode=AUTO;
    %let _chall_presample_cells=&gb_presample_max_cells.;
    %if %length(%superq(_chall_presample_cells))=0 %then
        %let _chall_presample_cells=25000000;
    %let _chall_seed=&gb_seed.;
    %if %length(%superq(_chall_seed))=0 %then %let _chall_seed=12345;

    %_chall_presample_work(input_data=work._chall_train_raw,
        target=&_chall_target., byvar=&_chall_byvar.,
        enabled=&_chall_presample_mode., max_cells=&_chall_presample_cells.,
        seed=&_chall_seed., out_data=work._chall_train_base,
        out_total_cells=_chall_total_cells,
        out_sampling_ratio=_chall_sampling_ratio,
        out_sampled_flag=_chall_sampled_flag);

    %_chall_publish_work(work_data=work._chall_train_base,
        cas_table=_chall_train_base);
    %_chall_publish_work(work_data=work._chall_oot_raw, cas_table=_chall_oot_base);

    %let _chall_partition_pct=&gb_partition_pct.;
    %if %length(%superq(_chall_partition_pct))=0 %then %let _chall_partition_pct=70;

    %_chall_build_partition(train_data=work._chall_train_base,
        oot_data=work._chall_oot_raw, target=&_chall_target.,
        byvar=&_chall_byvar., partition_pct=&_chall_partition_pct.,
        seed=&_chall_seed., out_train_part=work._chall_train_part,
        out_train=work._chall_train, out_valid=work._chall_valid,
        out_testoot=work._chall_testoot, out_full=work._chall_full_data);

    %_chall_publish_work(work_data=work._chall_train_part,
        cas_table=_chall_train_part);
    %_chall_publish_work(work_data=work._chall_train, cas_table=_chall_train);
    %_chall_publish_work(work_data=work._chall_valid, cas_table=_chall_valid);
    %_chall_publish_work(work_data=work._chall_testoot, cas_table=_chall_testoot);
    %_chall_publish_work(work_data=work._chall_full_data, cas_table=_chall_full_data);

    %let _chall_penalty_lambda=&gb_penalty_lambda.;
    %if %length(%superq(_chall_penalty_lambda))=0 %then
        %let _chall_penalty_lambda=0.5;

    %_chall_build_benchmark(train_data=work._chall_train_base,
        oot_data=work._chall_oot_raw, full_data=work._chall_full_data,
        target=&_chall_target., score_var=&_chall_score.,
        byvar=&_chall_byvar., penalty_lambda=&_chall_penalty_lambda.,
        out_global=work._chall_benchmark_global,
        out_monthly=work._chall_benchmark_monthly);

    %let _chall_gb_stagnation=&gb_stagnation.;
    %if %length(%superq(_chall_gb_stagnation))=0 %then
        %let _chall_gb_stagnation=0;
    %let _chall_ntrees_cap=&gb_ntrees_cap.;
    %if %length(%superq(_chall_ntrees_cap))=0 %then %let _chall_ntrees_cap=800;

    %_chall_gb_set_ranges(data=work._chall_train_part, num=&_chall_vars_num.,
        cat=&_chall_vars_cat., ntrees_cap=&_chall_ntrees_cap.);

    %let _chall_nparallel=5;

    %_chall_gb_tune(data=casuser._chall_train_part, num_input=&_chall_vars_num.,
        cat_input=&_chall_vars_cat., target_input=&_chall_target.,
        gb_stagnation=&_chall_gb_stagnation., nparallel=&_chall_nparallel.,
        eval_out=work._chall_eval_history,
        best_out=work._chall_bestconfiguration);

    data work._chall_eval_history;
        set work._chall_eval_history;
        if missing(Iteration) then Iteration=_n_;
    run;

    %let _chall_top_k=&gb_top_k.;
    %if %length(%superq(_chall_top_k))=0 %then %let _chall_top_k=40;
    %let _chall_top_models=&gb_top_models.;
    %if %length(%superq(_chall_top_models))=0 %then %let _chall_top_models=5;

    %_chall_prepare_topk(eval_data=work._chall_eval_history,
        top_k=&_chall_top_k., out=work._chall_topk_cfg);

    %let _chall_run_root=&fw_root./outputs/runs/&run_id.;
    %let _chall_models_subdir=models/METOD9;
    %let _chall_fw_root=&fw_root.;
    %let _chall_run_id=&run_id.;
    %let _chall_scope=&scope.;
    %let _chall_troncal_id=&troncal_id.;
    %let _chall_seg_id=&_chall_seg_num.;
    %let _chall_segment_label=&_chall_segment_lbl.;
    %let _chall_var_seg_name=&_chall_seg_final.;

    %_chall_run_refit_parallel(phase=TOPK, input_cfg=work._chall_topk_cfg,
        workers=5, result_out=work._chall_topk_results,
        persist_champion=0);

    proc sort data=work._chall_topk_results out=work._chall_topk_rank;
        by descending Gini_Penalizado;
    run;

    data work._chall_topk_rank;
        set work._chall_topk_rank;
        Model_Rank=_n_;
        Is_Champion=0;
    run;

    data work._chall_topn_cfg;
        set work._chall_topk_rank(obs=&_chall_top_models.);
        cfg_id=_n_;
        model_rank=_n_;
    run;

    %_chall_run_refit_parallel(phase=TOPN, input_cfg=work._chall_topn_cfg,
        workers=5, result_out=work._chall_topn_results,
        monthly_out=work._chall_monthly_models, persist_champion=1);

    proc sort data=work._chall_topn_results out=work._chall_topn_rank;
        by descending Gini_Penalizado;
    run;

    data work._chall_topn_rank;
        set work._chall_topn_rank;
        Model_Rank=_n_;
        cfg_id=_n_;
        Is_Champion=(Model_Rank=1);
    run;

    data work._chall_monthly_compare;
        set work._chall_benchmark_monthly work._chall_monthly_models;
    run;

    proc sort data=work._chall_monthly_compare;
        by Periodo Model_Rank;
    run;

    %_chall_build_registry(results=work._chall_topn_rank, troncal_id=&troncal_id.,
        scope=&scope., seg_id=&_chall_seg_num.,
        segment_label=&_chall_segment_lbl., var_seg=&_chall_seg_final.,
        astore_name=&_chall_astore_name.,
        models_path=&_chall_models_path./&_chall_astore_name..sashdat,
        algo_name=Gradient Boosting, algo_code=gb,
        artifact_prefix=&_chall_tbl_prefix.,
        out=work._chall_registry);

    %_chall_build_champion_summary(registry=work._chall_registry,
        benchmark=work._chall_benchmark_global,
        out=work._chall_champion_summary);

    libname _chalout "&_chall_tables_path.";

    proc sort data=work._chall_registry;
        by Model_Rank;
    run;

    data _chalout.&_chall_tbl_prefix._bmk;
        set work._chall_benchmark_global;
    run;
    data _chalout.&_chall_tbl_prefix._eval;
        set work._chall_eval_history;
    run;
    data _chalout.&_chall_tbl_prefix._topk;
        set work._chall_topk_rank;
    run;
    data _chalout.&_chall_tbl_prefix._topn;
        set work._chall_topn_rank;
    run;
    data _chalout.&_chall_tbl_prefix._mnly;
        set work._chall_monthly_compare;
    run;
    data _chalout.&_chall_tbl_prefix._rgst;
        set work._chall_registry;
    run;
    data _chalout.&_chall_tbl_prefix._chmp;
        set work._chall_champion_summary;
    run;

    libname _chalout clear;

    %_gradient_boosting_report(benchmark_data=work._chall_benchmark_global,
        eval_data=work._chall_eval_history, topk_data=work._chall_topk_rank,
        topn_data=work._chall_topn_rank,
        monthly_data=work._chall_monthly_compare,
        registry_data=work._chall_registry,
        champion_data=work._chall_champion_summary,
        report_path=&_chall_report_path., images_path=&_chall_images_path.,
        file_prefix=&_chall_report_prefix., model_type=&_chall_model_type.);

    proc datasets library=casuser nolist nowarn;
        delete _chall_:;
    quit;

    proc datasets library=work nolist nowarn;
        delete _chall_:;
    quit;

    %put NOTE:======================================================;
    %put NOTE: [gradient_boosting_run] FIN - &_chall_report_prefix.;
    %put NOTE:======================================================;
%mend gradient_boosting_run;
