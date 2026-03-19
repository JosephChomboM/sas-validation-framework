/* =========================================================================
decision_tree_parallel.sas - Paralelizacion y refit para DT Challenge
========================================================================= */

%macro _chall_drop_cas_table(cas_sess_name=conn, caslib_name=casuser,
    table_name=);
    %if %length(%superq(table_name))=0 %then %return;

    proc cas;
        session &cas_sess_name.;
        table.dropTable / caslib="&caslib_name."
            name="&table_name." quiet=true;
    quit;
%mend _chall_drop_cas_table;

%macro _chall_worker_refit(phase=TOPK, session_index=1, shard_table=,
    result_table=, monthly_table=, persist_champion=0);

    %local _phase_uc _n_models _m _cfg_id _maxlevel _nbins _leafsize
        _maxbranch _crit _missing _binmethod _prune _predvar _g_penal
        _worker_sess;

    %let _phase_uc=%upcase(%superq(phase));
    %let _worker_sess=casw&session_index.;

    proc sql noprint;
        select count(*) into :_n_models trimmed
        from &shard_table.;
    quit;

    proc sql;
        create table work._chall_worker_results (
            cfg_id num,
            model_rank num,
            maxlevel num,
            nbins num,
            leafsize num,
            maxbranch num,
            crit char(16),
            missing char(12),
            binmethod char(16),
            prune char(24),
            gini_train num,
            gini_oot num,
            gini_penalizado num,
            is_champion num
        );
    quit;

    %if &_phase_uc.=TOPN %then %do;
        data work._chall_worker_monthly;
            length Periodo 8 N_Total 8 N_Default 8 Gini 8 Model_Rank 8
                Model_Label $64 Algo_Name $32 Source $16;
            stop;
        run;
    %end;

    %do _m=1 %to &_n_models.;
        data _null_;
            set &shard_table.(firstobs=&_m. obs=&_m.);
            call symputx('_cfg_id', cfg_id, 'L');
            call symputx('_maxlevel', maxlevel, 'L');
            call symputx('_nbins', nbins, 'L');
            call symputx('_leafsize', leafsize, 'L');
            call symputx('_maxbranch', maxbranch, 'L');
            call symputx('_crit', crit, 'L');
            call symputx('_missing', missing, 'L');
            call symputx('_binmethod', binmethod, 'L');
            call symputx('_prune', prune, 'L');
        run;

        ods exclude all;
        proc treesplit data=casuser._chall_train_part
            assignmissing=&_missing.
            binmethod=&_binmethod.
            maxbranch=&_maxbranch.
            maxdepth=&_maxlevel.
            minleafsize=&_leafsize.
            numbin=&_nbins.
            seed=12345;
            grow &_crit.;
            prune &_prune.;
            partition rolevar=_PartInd_(train='1' validate='0' test='2');
            %if %length(%superq(_chall_vars_num)) > 0 %then %do;
                input &_chall_vars_num. / level=interval;
            %end;
            %if %length(%superq(_chall_vars_cat)) > 0 %then %do;
                input &_chall_vars_cat. / level=nominal;
            %end;
            target &_chall_target. / level=nominal;
            savestate rstore=casuser._chall_dt_w&session_index._cfg&_cfg_id.;
        run;

        proc astore;
            score data=casuser.&_chall_train_table.
                out=casuser._chall_trn_scd_w&session_index._cfg&_cfg_id.
                rstore=casuser._chall_dt_w&session_index._cfg&_cfg_id.
                copyvars=(&_chall_target. &_chall_byvar.);
        run;

        proc astore;
            score data=casuser.&_chall_oot_table.
                out=casuser._chall_oot_scd_w&session_index._cfg&_cfg_id.
                rstore=casuser._chall_dt_w&session_index._cfg&_cfg_id.
                copyvars=(&_chall_target. &_chall_byvar.);
        run;
        ods exclude none;

        %let _predvar=P_&_chall_target.1;

        data work._chall_train_scored;
            set casuser._chall_trn_scd_w&session_index._cfg&_cfg_id.;
        run;

        data work._chall_oot_scored;
            set casuser._chall_oot_scd_w&session_index._cfg&_cfg_id.;
        run;

        %_chall_gini(data=work._chall_train_scored, target=&_chall_target.,
            score_var=&_predvar., outmac=_chall_wrk_g_train);
        %_chall_gini(data=work._chall_oot_scored, target=&_chall_target.,
            score_var=&_predvar., outmac=_chall_wrk_g_oot);
        %let _g_penal=%sysevalf(&_chall_wrk_g_oot. -
            (&_chall_penalty_lambda. *
            (&_chall_wrk_g_train. - &_chall_wrk_g_oot.)));

        data work._chall_worker_row;
            cfg_id=&_cfg_id.;
            model_rank=&_cfg_id.;
            maxlevel=&_maxlevel.;
            nbins=&_nbins.;
            leafsize=&_leafsize.;
            maxbranch=&_maxbranch.;
            crit="&_crit.";
            missing="&_missing.";
            binmethod="&_binmethod.";
            prune="&_prune.";
            gini_train=&_chall_wrk_g_train.;
            gini_oot=&_chall_wrk_g_oot.;
            gini_penalizado=&_g_penal.;
            is_champion=(cfg_id=1);
            format gini_: 8.4;
        run;

        proc append base=work._chall_worker_results
            data=work._chall_worker_row force;
        run;

        %if &_phase_uc.=TOPN %then %do;
            data work._chall_full_scored;
                set work._chall_train_scored work._chall_oot_scored;
            run;

            %_chall_gini_monthly(data=work._chall_full_scored,
                target=&_chall_target., score_var=&_predvar.,
                byvar=&_chall_byvar., model_rank=&_cfg_id.,
                model_label=DT_&_cfg_id., algo_name=DecisionTree,
                source=CHALLENGER, out=work._chall_monthly_one);

            proc append base=work._chall_worker_monthly
                data=work._chall_monthly_one force;
            run;
        %end;

        %if &persist_champion.=1 and &_cfg_id.=1 %then %do;
            %_save_into_caslib(m_cas_sess_name=&_worker_sess.,
                m_input_caslib=casuser,
                m_input_data=_chall_dt_w&session_index._cfg&_cfg_id.,
                m_output_caslib=OUT,
                m_subdir_data=&_chall_models_subdir./&_chall_astore_name.);
        %end;
    %end;

    %_chall_drop_cas_table(cas_sess_name=&_worker_sess.,
        caslib_name=casuser, table_name=&result_table.);

    data casuser.&result_table.(copies=0 promote=yes);
        set work._chall_worker_results;
    run;

    %if &_phase_uc.=TOPN %then %do;
        %_chall_drop_cas_table(cas_sess_name=&_worker_sess.,
            caslib_name=casuser, table_name=&monthly_table.);

        data casuser.&monthly_table.(copies=0 promote=yes);
            set work._chall_worker_monthly;
        run;
    %end;

    proc datasets library=work nolist nowarn;
        delete _chall_worker_: _chall_train_scored _chall_oot_scored
            _chall_full_scored _chall_monthly_one;
    quit;
%mend _chall_worker_refit;

%macro _chall_run_refit_parallel(phase=TOPK, input_cfg=work._chall_topk_cfg,
    workers=5, result_out=work._chall_results, monthly_out=,
    persist_champion=0);

    %local _phase_uc _phase_lc _cfg_n _active_workers _w _result_name
        _monthly_name _task_name _log_path _task_prefix _remote_cas_sess
        _remote_shard_table _remote_result_table _remote_monthly_table
        _remote_session_index _remote_phase_uc _remote_persist_champion;

    %let _phase_uc=%upcase(%superq(phase));
    %let _phase_lc=%sysfunc(lowcase(%superq(phase)));
    %if &_phase_uc.=TOPK %then %let _task_prefix=CTK;
    %else %let _task_prefix=CTN;

    proc sql noprint;
        select count(*) into :_cfg_n trimmed from &input_cfg.;
    quit;

    %if %sysevalf(%superq(_cfg_n)=, boolean) or &_cfg_n.=0 %then %do;
        data &result_out.;
            stop;
        run;
        %if %length(%superq(monthly_out)) > 0 %then %do;
            data &monthly_out.;
                stop;
            run;
        %end;
        %return;
    %end;

    %let _active_workers=&workers.;
    %if %length(%superq(_active_workers))=0 %then %let _active_workers=5;
    %if &_active_workers. < 1 %then %let _active_workers=1;

    data work._chall_cfg_tagged;
        set &input_cfg.;
        worker_id=mod(_n_ - 1, &_active_workers.) + 1;
    run;

    %do _w=1 %to &_active_workers.;
        %_chall_drop_cas_table(cas_sess_name=conn, caslib_name=casuser,
            table_name=_chall_cfg_&_phase_lc._w&_w.);

        data casuser._chall_cfg_&_phase_lc._w&_w.(copies=0 promote=yes);
            set work._chall_cfg_tagged(where=(worker_id=&_w.));
            drop worker_id;
        run;
    %end;

    %if &_active_workers.=1 %then %do;
        %let _log_path=&_chall_run_root./logs/metod_9_&_chall_log_tag._&_phase_lc._worker_1.log;
        proc printto log="&_log_path." new;
        run;

        cas casw1 sessopts=(caslib="casuser");
        libname casuser cas caslib=casuser;
        options casdatalimit=ALL;

        %_chall_worker_refit(phase=&_phase_uc., session_index=1,
            shard_table=casuser._chall_cfg_&_phase_lc._w1,
            result_table=_chall_res_&_phase_lc._w1,
            monthly_table=_chall_mon_&_phase_lc._w1,
            persist_champion=&persist_champion.);

        proc printto;
        run;
        cas casw1 terminate;
    %end;
    %else %do;
        %do _w=1 %to &_active_workers.;
            %let _task_name=&_task_prefix.&_w.;
            %let _log_path=&_chall_run_root./logs/metod_9_&_chall_log_tag._&_phase_lc._worker_&_w..log;
            %let _remote_cas_sess=casw&_w.;
            %let _remote_shard_table=casuser._chall_cfg_&_phase_lc._w&_w.;
            %let _remote_result_table=_chall_res_&_phase_lc._w&_w.;
            %let _remote_monthly_table=_chall_mon_&_phase_lc._w&_w.;
            %let _remote_session_index=&_w.;
            %let _remote_phase_uc=&_phase_uc.;
            %let _remote_persist_champion=&persist_champion.;

            signon &_task_name. sascmd="!sascmd -nosyntaxcheck -noterminal";
            %syslput _global_ / like='*' remote=&_task_name.;
            %syslput _local_ / like='*' remote=&_task_name.;
            %syslput fw_root=&fw_root. / remote=&_task_name.;
            %syslput run_id=&run_id. / remote=&_task_name.;
            %syslput _log_path=&_log_path. / remote=&_task_name.;
            %syslput _remote_cas_sess=&_remote_cas_sess. / remote=&_task_name.;
            %syslput _remote_shard_table=&_remote_shard_table.
                / remote=&_task_name.;
            %syslput _remote_result_table=&_remote_result_table.
                / remote=&_task_name.;
            %syslput _remote_monthly_table=&_remote_monthly_table.
                / remote=&_task_name.;
            %syslput _remote_session_index=&_remote_session_index.
                / remote=&_task_name.;
            %syslput _remote_phase_uc=&_remote_phase_uc.
                / remote=&_task_name.;
            %syslput _remote_persist_champion=&_remote_persist_champion.
                / remote=&_task_name.;

            rsubmit &_task_name. wait=no;
                options MSGLEVEL=I NOFULLSTIMER OBS=MAX NOSYNTAXCHECK
                    REPLACE NOQUOTELENMAX;
                proc printto log="&_log_path." new;
                run;

                %include "&fw_root./src/common/cas_utils.sas";
                %include "&fw_root./src/modules/decision_tree/impl/decision_tree_compute.sas";
                %include "&fw_root./src/modules/decision_tree/impl/decision_tree_parallel.sas";

                cas &_remote_cas_sess. sessopts=(caslib="casuser");
                libname casuser cas caslib=casuser;
                options casdatalimit=ALL;

                %_chall_worker_refit(phase=&_remote_phase_uc.,
                    session_index=&_remote_session_index.,
                    shard_table=&_remote_shard_table.,
                    result_table=&_remote_result_table.,
                    monthly_table=&_remote_monthly_table.,
                    persist_champion=&_remote_persist_champion.);

                proc printto;
                run;
                cas &_remote_cas_sess. terminate;
            endrsubmit;
        %end;

        waitfor _ALL_
            %do _w=1 %to &_active_workers.;
                &_task_prefix.&_w.
            %end;
        ;

        %do _w=1 %to &_active_workers.;
            signoff &_task_prefix.&_w.;
        %end;
    %end;

    data &result_out.;
        set
        %do _w=1 %to &_active_workers.;
            casuser._chall_res_&_phase_lc._w&_w.
        %end;
        ;
    run;

    %if %length(%superq(monthly_out)) > 0 and &_phase_uc.=TOPN %then %do;
        data &monthly_out.;
            set
            %do _w=1 %to &_active_workers.;
                casuser._chall_mon_&_phase_lc._w&_w.
            %end;
            ;
        run;
    %end;

    proc datasets library=work nolist nowarn;
        delete _chall_cfg_tagged;
    quit;
%mend _chall_run_refit_parallel;
