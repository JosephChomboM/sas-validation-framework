/* =========================================================================
calibracion_run.sas - Macro publica del modulo Calibracion (METOD8)
========================================================================= */
%include "&fw_root./src/modules/calibracion/calibracion_contract.sas";
%include "&fw_root./src/modules/calibracion/impl/calibracion_compute.sas";
%include "&fw_root./src/modules/calibracion/impl/calibracion_report.sas";

%macro calibracion_run(input_caslib=PROC, train_table=_train_input,
    oot_table=_oot_input, output_caslib=OUT, troncal_id=, scope=, run_id=);

    %global _cal_rc;
    %let _cal_rc=0;

    %local _cal_target _cal_score _cal_pd _cal_xb _cal_monto _cal_byvar
        _cal_def_cld _cal_vars_num _cal_vars_cat _cal_is_custom _cal_scope_abbr
        _cal_report_path _cal_images_path _cal_tables_path _cal_file_prefix
        _cal_tbl_prefix _cal_seg_num _cal_dir_rc _cal_groups _cal_keep_train
        _cal_keep_oot _cal_keep_train_sql _cal_keep_oot_sql
        _cal_driver_keep_train _cal_driver_keep_oot _cal_any_train
        _cal_weighted _cal_score_mode _cal_cfg_num _cal_cfg_cat
        _cal_cfg_dri_num _cal_cfg_dri_cat _cal_merge_idx _cal_merge_var;

    %put NOTE:======================================================;
    %put NOTE: [calibracion_run] INICIO;
    %put NOTE: troncal=&troncal_id. scope=&scope.;
    %put NOTE: train=&input_caslib..&train_table.;
    %put NOTE: oot=&input_caslib..&oot_table.;
    %put NOTE:======================================================;

    %let _cal_target=;
    %let _cal_score=;
    %let _cal_pd=;
    %let _cal_xb=;
    %let _cal_monto=;
    %let _cal_byvar=;
    %let _cal_def_cld=;
    %let _cal_vars_num=;
    %let _cal_vars_cat=;
    %let _cal_cfg_num=;
    %let _cal_cfg_cat=;
    %let _cal_cfg_dri_num=;
    %let _cal_cfg_dri_cat=;
    %let _cal_is_custom=0;
    %let _cal_weighted=0;

    proc sql noprint;
        select strip(target) into :_cal_target trimmed from
            casuser.cfg_troncales where troncal_id=&troncal_id.;
        select strip(pd) into :_cal_pd trimmed from casuser.cfg_troncales
            where troncal_id=&troncal_id.;
        select strip(xb) into :_cal_xb trimmed from casuser.cfg_troncales
            where troncal_id=&troncal_id.;
        select strip(monto) into :_cal_monto trimmed from casuser.cfg_troncales
            where troncal_id=&troncal_id.;
        select strip(byvar) into :_cal_byvar trimmed from
            casuser.cfg_troncales where troncal_id=&troncal_id.;
        select strip(put(def_cld, best.)) into :_cal_def_cld trimmed from
            casuser.cfg_troncales where troncal_id=&troncal_id.;
    quit;

    %let _cal_score_mode=%upcase(%superq(cal_score_source));
    %if %length(%superq(_cal_score_mode))=0 %then %let _cal_score_mode=AUTO;

    %if %upcase(&cal_mode.)=CUSTOM %then %do;
        %if %length(%superq(cal_custom_vars_dri_num)) > 0 or
            %length(%superq(cal_custom_vars_dri_cat)) > 0 %then %do;
            %let _cal_vars_num=&cal_custom_vars_dri_num.;
            %let _cal_vars_cat=&cal_custom_vars_dri_cat.;
            %let _cal_is_custom=1;

            %if %length(%superq(cal_custom_target)) > 0 %then
                %let _cal_target=&cal_custom_target.;
            %if %length(%superq(cal_custom_monto)) > 0 %then
                %let _cal_monto=&cal_custom_monto.;
            %if %length(%superq(cal_custom_def_cld)) > 0 %then
                %let _cal_def_cld=&cal_custom_def_cld.;
            %if %length(%superq(cal_custom_score_var)) > 0 %then
                %let _cal_score=&cal_custom_score_var.;

            %put NOTE: [calibracion_run] Modo CUSTOM activado.;
        %end;
        %else %do;
            %put WARNING: [calibracion_run] cal_mode=CUSTOM pero sin drivers
                custom. Fallback a AUTO.;
        %end;
    %end;

    %if %length(%superq(_cal_score))=0 %then %do;
        %if &_cal_score_mode.=PD %then %let _cal_score=&_cal_pd.;
        %else %if &_cal_score_mode.=XB %then %let _cal_score=&_cal_xb.;
        %else %if &_cal_score_mode.=CUSTOM %then %do;
            %if %length(%superq(cal_custom_score_var)) > 0 %then
                %let _cal_score=&cal_custom_score_var.;
            %else %do;
                %put WARNING: [calibracion_run] cal_score_source=CUSTOM sin
                    cal_custom_score_var. Fallback a AUTO.;
            %end;
        %end;
    %end;

    %if %length(%superq(_cal_score))=0 %then %do;
        %if %length(%superq(_cal_pd)) > 0 %then %let _cal_score=&_cal_pd.;
        %else %let _cal_score=&_cal_xb.;
    %end;

    %if &_cal_is_custom.=0 %then %do;
        %if %substr(&scope., 1, 3)=seg %then %do;
            %let _cal_seg_num=%sysfunc(inputn(%substr(&scope., 4), best.));
            proc sql noprint;
                select strip(num_list) into :_cal_cfg_num trimmed from
                    casuser.cfg_segmentos where troncal_id=&troncal_id.
                    and seg_id=&_cal_seg_num.;
                select strip(cat_list) into :_cal_cfg_cat trimmed from
                    casuser.cfg_segmentos where troncal_id=&troncal_id.
                    and seg_id=&_cal_seg_num.;
                select strip(dri_num_list) into :_cal_cfg_dri_num trimmed from
                    casuser.cfg_segmentos where troncal_id=&troncal_id.
                    and seg_id=&_cal_seg_num.;
                select strip(dri_cat_list) into :_cal_cfg_dri_cat trimmed from
                    casuser.cfg_segmentos where troncal_id=&troncal_id.
                    and seg_id=&_cal_seg_num.;
            quit;
        %end;

        %if %length(%superq(_cal_cfg_num))=0 %then %do;
            proc sql noprint;
                select strip(num_unv) into :_cal_cfg_num trimmed from
                    casuser.cfg_troncales where troncal_id=&troncal_id.;
            quit;
        %end;

        %if %length(%superq(_cal_cfg_cat))=0 %then %do;
            proc sql noprint;
                select strip(cat_unv) into :_cal_cfg_cat trimmed from
                    casuser.cfg_troncales where troncal_id=&troncal_id.;
            quit;
        %end;

        %if %length(%superq(_cal_cfg_dri_num))=0 %then %do;
            proc sql noprint;
                select strip(dri_num_unv) into :_cal_cfg_dri_num trimmed from
                    casuser.cfg_troncales where troncal_id=&troncal_id.;
            quit;
        %end;

        %if %length(%superq(_cal_cfg_dri_cat))=0 %then %do;
            proc sql noprint;
                select strip(dri_cat_unv) into :_cal_cfg_dri_cat trimmed from
                    casuser.cfg_troncales where troncal_id=&troncal_id.;
            quit;
        %end;

        %let _cal_merge_idx=1;
        %let _cal_merge_var=%scan(%superq(_cal_cfg_num), &_cal_merge_idx.,
            %str( ));
        %do %while(%length(%superq(_cal_merge_var)) > 0);
            %_cal_push_unique(list_name=_cal_vars_num, value=&_cal_merge_var.);
            %let _cal_merge_idx=%eval(&_cal_merge_idx. + 1);
            %let _cal_merge_var=%scan(%superq(_cal_cfg_num), &_cal_merge_idx.,
                %str( ));
        %end;

        %let _cal_merge_idx=1;
        %let _cal_merge_var=%scan(%superq(_cal_cfg_dri_num), &_cal_merge_idx.,
            %str( ));
        %do %while(%length(%superq(_cal_merge_var)) > 0);
            %_cal_push_unique(list_name=_cal_vars_num, value=&_cal_merge_var.);
            %let _cal_merge_idx=%eval(&_cal_merge_idx. + 1);
            %let _cal_merge_var=%scan(%superq(_cal_cfg_dri_num),
                &_cal_merge_idx., %str( ));
        %end;

        %let _cal_merge_idx=1;
        %let _cal_merge_var=%scan(%superq(_cal_cfg_cat), &_cal_merge_idx.,
            %str( ));
        %do %while(%length(%superq(_cal_merge_var)) > 0);
            %_cal_push_unique(list_name=_cal_vars_cat, value=&_cal_merge_var.);
            %let _cal_merge_idx=%eval(&_cal_merge_idx. + 1);
            %let _cal_merge_var=%scan(%superq(_cal_cfg_cat), &_cal_merge_idx.,
                %str( ));
        %end;

        %let _cal_merge_idx=1;
        %let _cal_merge_var=%scan(%superq(_cal_cfg_dri_cat), &_cal_merge_idx.,
            %str( ));
        %do %while(%length(%superq(_cal_merge_var)) > 0);
            %_cal_push_unique(list_name=_cal_vars_cat, value=&_cal_merge_var.);
            %let _cal_merge_idx=%eval(&_cal_merge_idx. + 1);
            %let _cal_merge_var=%scan(%superq(_cal_cfg_dri_cat),
                &_cal_merge_idx., %str( ));
        %end;
    %end;

    %_cal_push_unique(list_name=_cal_vars_cat, value=&_cal_byvar.);

    %let _cal_groups=&cal_groups.;
    %if %length(%superq(_cal_groups))=0 %then %let _cal_groups=5;
    %if %sysevalf(&_cal_groups. < 1) %then %let _cal_groups=5;

    %put NOTE: [calibracion_run] Variables resueltas:;
    %put NOTE: [calibracion_run] target=&_cal_target. score=&_cal_score.;
    %put NOTE: [calibracion_run] monto=&_cal_monto. byvar=&_cal_byvar.
        def_cld=&_cal_def_cld.;
    %put NOTE: [calibracion_run] dri_num=&_cal_vars_num.;
    %put NOTE: [calibracion_run] dri_cat=&_cal_vars_cat.;
    %put NOTE: [calibracion_run] groups=&_cal_groups.;

    %if %substr(&scope., 1, 3)=seg %then %let _cal_scope_abbr=&scope.;
    %else %let _cal_scope_abbr=base;

    %if &_cal_is_custom.=1 %then %do;
        %let _cal_report_path=&fw_root./outputs/runs/&run_id./experiments;
        %let _cal_images_path=&fw_root./outputs/runs/&run_id./experiments;
        %let _cal_tables_path=&fw_root./outputs/runs/&run_id./experiments;
        %let _cal_file_prefix=custom_calibracion_troncal_&troncal_id._&_cal_scope_abbr.;
        %let _cal_tbl_prefix=cx_calb_t&troncal_id._&_cal_scope_abbr.;
        %put NOTE: [calibracion_run] Output -> experiments/ (exploratorio).;
    %end;
    %else %do;
        %let _cal_report_path=&fw_root./outputs/runs/&run_id./reports/METOD8;
        %let _cal_images_path=&fw_root./outputs/runs/&run_id./images/METOD8;
        %let _cal_tables_path=&fw_root./outputs/runs/&run_id./tables/METOD8;
        %let _cal_file_prefix=calibracion_troncal_&troncal_id._&_cal_scope_abbr.;
        %let _cal_tbl_prefix=calb_t&troncal_id._&_cal_scope_abbr.;
        %let _cal_dir_rc=%sysfunc(dcreate(METOD8,
            &fw_root./outputs/runs/&run_id./reports));
        %let _cal_dir_rc=%sysfunc(dcreate(METOD8,
            &fw_root./outputs/runs/&run_id./images));
        %let _cal_dir_rc=%sysfunc(dcreate(METOD8,
            &fw_root./outputs/runs/&run_id./tables));
        %put NOTE: [calibracion_run] Output -> reports/images/tables METOD8.;
    %end;

    %calibracion_contract(input_caslib=&input_caslib.,
        train_table=&train_table., oot_table=&oot_table.,
        vars_num=&_cal_vars_num., vars_cat=&_cal_vars_cat.,
        target=&_cal_target., score_var=&_cal_score., byvar=&_cal_byvar.,
        def_cld=&_cal_def_cld., monto_var=&_cal_monto.);

    %if &_cal_rc. ne 0 %then %do;
        %put ERROR: [calibracion_run] Contract fallido - modulo abortado.;
        %return;
    %end;

    %_cal_build_driver_meta(data_train=&input_caslib..&train_table.,
        data_oot=&input_caslib..&oot_table., vars_num=&_cal_vars_num.,
        vars_cat=&_cal_vars_cat., out=work._cal_driver_meta,
        out_keep_train=_cal_driver_keep_train,
        out_keep_oot=_cal_driver_keep_oot,
        out_any_train=_cal_any_train);

    %if %sysfunc(inputn(&_cal_any_train., best32.))=0 %then %do;
        %put ERROR: [calibracion_run] Ningun driver valido existe en TRAIN.;
        %let _cal_rc=1;
        %return;
    %end;

    %let _cal_keep_train=;
    %_cal_push_unique(list_name=_cal_keep_train, value=&_cal_target.);
    %_cal_push_unique(list_name=_cal_keep_train, value=&_cal_score.);
    %_cal_push_unique(list_name=_cal_keep_train, value=&_cal_byvar.);
    %if %length(%superq(_cal_monto)) > 0 %then
        %_cal_push_unique(list_name=_cal_keep_train, value=&_cal_monto.);
    %let _cal_merge_idx=1;
    %let _cal_merge_var=%scan(%superq(_cal_driver_keep_train),
        &_cal_merge_idx., %str( ));
    %do %while(%length(%superq(_cal_merge_var)) > 0);
        %_cal_push_unique(list_name=_cal_keep_train, value=&_cal_merge_var.);
        %let _cal_merge_idx=%eval(&_cal_merge_idx. + 1);
        %let _cal_merge_var=%scan(%superq(_cal_driver_keep_train),
            &_cal_merge_idx., %str( ));
    %end;
    %let _cal_keep_train_sql=%sysfunc(tranwrd(%superq(_cal_keep_train),
        %str( ), %str(, )));

    %let _cal_keep_oot=;
    %_cal_push_unique(list_name=_cal_keep_oot, value=&_cal_target.);
    %_cal_push_unique(list_name=_cal_keep_oot, value=&_cal_score.);
    %_cal_push_unique(list_name=_cal_keep_oot, value=&_cal_byvar.);
    %if %length(%superq(_cal_monto)) > 0 %then
        %_cal_push_unique(list_name=_cal_keep_oot, value=&_cal_monto.);
    %let _cal_merge_idx=1;
    %let _cal_merge_var=%scan(%superq(_cal_driver_keep_oot),
        &_cal_merge_idx., %str( ));
    %do %while(%length(%superq(_cal_merge_var)) > 0);
        %_cal_push_unique(list_name=_cal_keep_oot, value=&_cal_merge_var.);
        %let _cal_merge_idx=%eval(&_cal_merge_idx. + 1);
        %let _cal_merge_var=%scan(%superq(_cal_driver_keep_oot),
            &_cal_merge_idx., %str( ));
    %end;
    %let _cal_keep_oot_sql=%sysfunc(tranwrd(%superq(_cal_keep_oot),
        %str( ), %str(, )));

    proc fedsql sessref=conn;
        create table casuser._cal_train {options replace=true} as
            select &_cal_keep_train_sql.
            from &input_caslib..&train_table.
            where &_cal_byvar. <= &_cal_def_cld.;
    quit;

    proc fedsql sessref=conn;
        create table casuser._cal_oot {options replace=true} as
            select &_cal_keep_oot_sql.
            from &input_caslib..&oot_table.
            where &_cal_byvar. <= &_cal_def_cld.;
    quit;

    %if (%upcase(&cal_use_weighted.)=1 or %upcase(&cal_use_weighted.)=YES)
        and %length(%superq(_cal_monto)) > 0 %then %do;
        %let _cal_weighted=1;
    %end;

    %if &_cal_weighted.=1 %then %do;
        %local _cal_has_w_trn _cal_has_w_oot;
        %let _cal_has_w_trn=0;
        %let _cal_has_w_oot=0;
        %_cal_var_exists(data=casuser._cal_train, var=&_cal_monto.,
            outvar=_cal_has_w_trn);
        %_cal_var_exists(data=casuser._cal_oot, var=&_cal_monto.,
            outvar=_cal_has_w_oot);
        %if &_cal_has_w_trn.=0 or &_cal_has_w_oot.=0 %then %do;
            %put WARNING: [calibracion_run] monto=&_cal_monto. no disponible
                en ambos splits filtrados. Se omite la variante ponderada.;
            %let _cal_weighted=0;
        %end;
    %end;

    %_calibration_compute(train_data=casuser._cal_train, oot_data=casuser._cal_oot,
        driver_meta=work._cal_driver_meta, target=&_cal_target.,
        score_var=&_cal_score., weight_var=&_cal_monto., groups=&_cal_groups.,
        calc_weighted=&_cal_weighted., out_detail=casuser._cal_detail,
        out_cuts=casuser._cal_cuts);

    libname _calout "&_cal_tables_path.";

    data work._cal_detl_out;
        set casuser._cal_detail;
    run;

    proc sort data=work._cal_detl_out out=_calout.&_cal_tbl_prefix._detl;
        by Var_Seq Split Calc_Mode Bucket_Order;
    run;

    data work._cal_cuts_out;
        set casuser._cal_cuts;
    run;

    proc sort data=work._cal_cuts_out out=_calout.&_cal_tbl_prefix._cuts;
        by Var_Seq Bucket_Order;
    run;

    %_calibracion_report(detail_data=_calout.&_cal_tbl_prefix._detl,
        cuts_data=_calout.&_cal_tbl_prefix._cuts,
        report_path=&_cal_report_path., images_path=&_cal_images_path.,
        file_prefix=&_cal_file_prefix., weighted=&_cal_weighted.);

    libname _calout clear;

    proc datasets library=casuser nolist nowarn;
        delete _cal_:;
    quit;

    proc datasets library=work nolist nowarn;
        delete _cal_:;
    quit;

    %put NOTE:======================================================;
    %put NOTE: [calibracion_run] FIN - &_cal_file_prefix.
        (mode=&cal_mode.);
    %put NOTE:======================================================;

%mend calibracion_run;
