/* =========================================================================
challenge_run.sas - Orquestador final de Challenge (METOD9)
Consolida resultados por algoritmo y selecciona el champion final.
========================================================================= */
%include "&fw_root./src/modules/challenge/impl/challenge_champion.sas";
%include "&fw_root./src/modules/challenge/impl/challenge_report.sas";

%macro _chall_final_has_table(libref=, memname=, outvar=_chall_has_table);
    proc sql noprint;
        select count(*) into :&outvar. trimmed
        from dictionary.tables
        where upcase(libname)=upcase("&libref.")
          and upcase(memname)=upcase("&memname.");
    quit;
%mend _chall_final_has_table;

%macro _chall_final_drop_table(caslib=, table=, sess=conn);
    %if %length(%superq(table))=0 %then %return;
    proc cas;
        session &sess.;
        table.dropTable / caslib="&caslib." name="&table." quiet=true;
    quit;
%mend _chall_final_drop_table;

%macro _chall_final_load_inputs(troncal_id=, seg_id=,
    out_train=_chall_final_train, out_oot=_chall_final_oot);

    %local _train_path _oot_path;

    %fw_path_processed(outvar=_train_path, troncal_id=&troncal_id.,
        split=train, seg_id=&seg_id.);
    %fw_path_processed(outvar=_oot_path, troncal_id=&troncal_id.,
        split=oot, seg_id=&seg_id.);

    %_promote_castable(m_cas_sess_name=conn, m_input_caslib=PROC,
        m_subdir_data=&_train_path., m_output_caslib=PROC,
        m_output_data=&out_train.);
    %_promote_castable(m_cas_sess_name=conn, m_input_caslib=PROC,
        m_subdir_data=&_oot_path., m_output_caslib=PROC,
        m_output_data=&out_oot.);
%mend _chall_final_load_inputs;

%macro _chall_final_load_model(astore_name=, out_table=_chall_final_rstore);
    %_promote_castable(m_cas_sess_name=conn, m_input_caslib=OUT,
        m_subdir_data=models/METOD9/&astore_name., m_output_caslib=OUT,
        m_output_data=&out_table.);
%mend _chall_final_load_model;

%macro challenge_run(input_caslib=PROC, train_table=_train_input,
    oot_table=_oot_input, output_caslib=OUT, troncal_id=, scope=, run_id=);

    %local _chall_target _chall_pd _chall_xb _chall_score _chall_byvar
        _chall_var_seg _chall_model_type _chall_n_segments _chall_scope_abbr
        _chall_report_path _chall_images_path _chall_tables_path
        _chall_dir_rc _chall_report_prefix _chall_tbl_prefix _prefix_root
        _chall_mode_eff _model_low _model_high _algo_codes _i _j _algo_code
        _algo_prefix _scope_has_registry _registry_sets _current_scope
        _segmented_universe _registry_appended _selected_appended
        _selected_n _scope_seg_id _champ_mode _copyvars _bmk_score_var
        _chall_seg_num _rows_train_base _rows_train_scored _rows_oot_base
        _rows_oot_scored _sel_astore _sel_algo;

    %put NOTE:======================================================;
    %put NOTE: [challenge_run] INICIO - troncal=&troncal_id. scope=&scope.;
    %put NOTE:======================================================;

    proc sql noprint;
        select strip(target), strip(pd), strip(xb), strip(byvar),
               strip(var_seg), strip(model_type), n_segments
          into :_chall_target trimmed,
               :_chall_pd trimmed,
               :_chall_xb trimmed,
               :_chall_byvar trimmed,
               :_chall_var_seg trimmed,
               :_chall_model_type trimmed,
               :_chall_n_segments trimmed
        from casuser.cfg_troncales
        where troncal_id=&troncal_id.;
    quit;

    %if %length(%superq(_chall_model_type))=0 %then %let _chall_model_type=BHV;
    %if %length(%superq(_chall_n_segments))=0 %then %let _chall_n_segments=0;

    %let _chall_score=&_chall_pd.;
    %if %length(%superq(_chall_score))=0 %then %let _chall_score=&_chall_xb.;
    %let _bmk_score_var=&_chall_score.;

    %let _chall_mode_eff=%upcase(%superq(challenge_mode));
    %if %length(%superq(_chall_mode_eff))=0 %then %let _chall_mode_eff=AUTO;

    %if %substr(&scope., 1, 3)=seg %then %do;
        %let _chall_seg_num=%sysfunc(inputn(%substr(&scope., 4), best.));
        %let _chall_scope_abbr=&scope.;
    %end;
    %else %do;
        %let _chall_seg_num=.;
        %let _chall_scope_abbr=base;
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
        %let _chall_report_prefix=custom_challenge_troncal_&troncal_id._&_chall_scope_abbr.;
        %let _chall_tbl_prefix=cx_chall_t&troncal_id._&_chall_scope_abbr.;
        %let _prefix_root=cx_;
    %end;
    %else %do;
        %let _chall_report_path=&fw_root./outputs/runs/&run_id./reports/METOD9;
        %let _chall_images_path=&fw_root./outputs/runs/&run_id./images/METOD9;
        %let _chall_tables_path=&fw_root./outputs/runs/&run_id./tables/METOD9;
        %let _chall_report_prefix=challenge_troncal_&troncal_id._&_chall_scope_abbr.;
        %let _chall_tbl_prefix=chall_t&troncal_id._&_chall_scope_abbr.;
        %let _chall_dir_rc=%sysfunc(dcreate(METOD9,
            &fw_root./outputs/runs/&run_id./reports));
        %let _chall_dir_rc=%sysfunc(dcreate(METOD9,
            &fw_root./outputs/runs/&run_id./images));
        %let _chall_dir_rc=%sysfunc(dcreate(METOD9,
            &fw_root./outputs/runs/&run_id./tables));
    %end;

    libname _challin "&_chall_tables_path.";
    %let _algo_codes=gb dt rf;
    %let _segmented_universe=0;
    %let _registry_appended=0;
    %let _selected_appended=0;

    %if %upcase(&scope.)=BASE and &_chall_n_segments. > 0 and
        %length(%superq(_chall_var_seg)) > 0 %then %do;
        %do _i=1 %to &_chall_n_segments.;
            %let _current_scope=seg%sysfunc(putn(&_i., z3.));
            %let _scope_has_registry=0;
            %do _j=1 %to %sysfunc(countw(&_algo_codes., %str( )));
                %let _algo_code=%scan(&_algo_codes., &_j., %str( ));
                %let _algo_prefix=&_prefix_root.&_algo_code._t&troncal_id._&_current_scope.;
                %_chall_final_has_table(libref=_challin,
                    memname=&_algo_prefix._rgst, outvar=_scope_has_registry);
                %if &_scope_has_registry. > 0 %then %do;
                    %let _segmented_universe=1;
                    %goto _chall_segmented_universe_found;
                %end;
            %end;
        %end;
    %end;
%_chall_segmented_universe_found:

    %if &_segmented_universe.=1 %then %do;
        %put NOTE: [challenge_run] Modo universo segmentado activado.
            Se seleccionara un champion por segmento y luego se scoreara
            el universo por append de segmentos.;

        %do _i=1 %to &_chall_n_segments.;
            %let _current_scope=seg%sysfunc(putn(&_i., z3.));
            %let _registry_sets=;

            %do _j=1 %to %sysfunc(countw(&_algo_codes., %str( )));
                %let _algo_code=%scan(&_algo_codes., &_j., %str( ));
                %let _algo_prefix=&_prefix_root.&_algo_code._t&troncal_id._&_current_scope.;
                %let _scope_has_registry=0;
                %_chall_final_has_table(libref=_challin,
                    memname=&_algo_prefix._rgst, outvar=_scope_has_registry);
                %if &_scope_has_registry. > 0 %then %do;
                    %if %length(%superq(_registry_sets))=0 %then
                        %let _registry_sets=_challin.&_algo_prefix._rgst;
                    %else
                        %let _registry_sets=&_registry_sets. _challin.&_algo_prefix._rgst;
                %end;
            %end;

            %if %length(%superq(_registry_sets))=0 %then %do;
                %put WARNING: [challenge_run] No se encontraron registries para
                    &_current_scope.. Se omitira del champion universo.;
            %end;
            %else %do;
                data work._chall_registry_scope;
                    set &_registry_sets.;
                run;

                %if &_registry_appended.=0 %then %do;
                    data work._chall_registry;
                        set work._chall_registry_scope;
                    run;
                    %let _registry_appended=1;
                %end;
                %else %do;
                    proc append base=work._chall_registry
                        data=work._chall_registry_scope force;
                    run;
                %end;

                proc sort data=work._chall_registry_scope(where=(Is_Champion=1))
                    out=work._chall_scope_champion;
                    by descending Gini_Penalizado descending Gini_OOT
                        descending Gini_Train;
                run;

                data work._chall_scope_champion;
                    set work._chall_scope_champion(obs=1);
                run;

                %if &_selected_appended.=0 %then %do;
                    data work._chall_selected_models;
                        set work._chall_scope_champion;
                    run;
                    %let _selected_appended=1;
                %end;
                %else %do;
                    proc append base=work._chall_selected_models
                        data=work._chall_scope_champion force;
                    run;
                %end;
            %end;
        %end;
    %end;
    %else %do;
        %let _registry_sets=;
        %do _i=1 %to %sysfunc(countw(&_algo_codes., %str( )));
            %let _algo_code=%scan(&_algo_codes., &_i., %str( ));
            %let _algo_prefix=&_prefix_root.&_algo_code._t&troncal_id._&_chall_scope_abbr.;
            %let _scope_has_registry=0;
            %_chall_final_has_table(libref=_challin,
                memname=&_algo_prefix._rgst, outvar=_scope_has_registry);
            %if &_scope_has_registry. > 0 %then %do;
                %if %length(%superq(_registry_sets))=0 %then
                    %let _registry_sets=_challin.&_algo_prefix._rgst;
                %else
                    %let _registry_sets=&_registry_sets. _challin.&_algo_prefix._rgst;
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

        proc sort data=work._chall_registry(where=(Is_Champion=1))
            out=work._chall_selected_models;
            by descending Gini_Penalizado descending Gini_OOT
                descending Gini_Train;
        run;

        data work._chall_selected_models;
            set work._chall_selected_models(obs=1);
        run;
    %end;

    %if &_registry_appended.=0 and &_segmented_universe.=1 %then %do;
        %put WARNING: [challenge_run] No hubo registries validos por segmento.
            No se puede calcular champion universo.;
        libname _challin clear;
        %return;
    %end;

    data work._chall_algo_champions;
        set work._chall_registry(where=(Is_Champion=1));
    run;

    proc sql noprint;
        select count(*) into :_selected_n trimmed
        from work._chall_selected_models;
    quit;

    %if %sysevalf(%superq(_selected_n)=, boolean) or &_selected_n.=0 %then %do;
        %put WARNING: [challenge_run] No se pudo seleccionar un champion final
            para troncal=&troncal_id. scope=&scope..;
        libname _challin clear;
        %return;
    %end;

    %let _copyvars=&_chall_target. &_chall_byvar.;
    %if %length(%superq(_chall_var_seg)) > 0 %then
        %let _copyvars=&_copyvars. &_chall_var_seg.;

    %if &_segmented_universe.=1 %then %do;
        %_chall_final_load_inputs(troncal_id=&troncal_id., seg_id=,
            out_train=_chall_final_train, out_oot=_chall_final_oot);
        %let _champ_mode=SEGMENT_APPEND;

        proc sql noprint;
            select seg_id, strip(Astore_Name), strip(Algo_Name)
              into :_sel_seg1-:_sel_seg%left(&_selected_n.),
                   :_sel_astore1-:_sel_astore%left(&_selected_n.),
                   :_sel_algo1-:_sel_algo%left(&_selected_n.)
            from work._chall_selected_models
            order by seg_id;
        quit;

        %do _i=1 %to &_selected_n.;
            %_chall_final_load_model(astore_name=&&_sel_astore&_i.,
                out_table=_chall_final_rstore_&_i.);

            data PROC._chall_seg_train_&_i.(copies=0);
                set PROC._chall_final_train(where=(&_chall_var_seg.=&&_sel_seg&_i.));
            run;
            data PROC._chall_seg_oot_&_i.(copies=0);
                set PROC._chall_final_oot(where=(&_chall_var_seg.=&&_sel_seg&_i.));
            run;

            proc astore;
                score data=PROC._chall_seg_train_&_i.
                    out=casuser._chall_scored_train_raw_&_i.
                    rstore=OUT._chall_final_rstore_&_i.
                    copyvars=(&_copyvars.);
            run;

            proc astore;
                score data=PROC._chall_seg_oot_&_i.
                    out=casuser._chall_scored_oot_raw_&_i.
                    rstore=OUT._chall_final_rstore_&_i.
                    copyvars=(&_copyvars.);
            run;

            data casuser._chall_scored_train_&_i.(copies=0);
                length Algo_Name $32 Astore_Name $128;
                set casuser._chall_scored_train_raw_&_i.;
                PD_FINAL=P_&_chall_target.1;
                Algo_Name="&&_sel_algo&_i.";
                Astore_Name="&&_sel_astore&_i.";
                drop P_&_chall_target.0 P_&_chall_target.1;
            run;

            data casuser._chall_scored_oot_&_i.(copies=0);
                length Algo_Name $32 Astore_Name $128;
                set casuser._chall_scored_oot_raw_&_i.;
                PD_FINAL=P_&_chall_target.1;
                Algo_Name="&&_sel_algo&_i.";
                Astore_Name="&&_sel_astore&_i.";
                drop P_&_chall_target.0 P_&_chall_target.1;
            run;
        %end;

        data casuser._chall_scored_train(copies=0);
            set
            %do _i=1 %to &_selected_n.;
                casuser._chall_scored_train_&_i.
            %end;
            ;
        run;

        data casuser._chall_scored_oot(copies=0);
            set
            %do _i=1 %to &_selected_n.;
                casuser._chall_scored_oot_&_i.
            %end;
            ;
        run;
    %end;
    %else %do;
        %let _scope_seg_id=;
        %if %substr(&scope., 1, 3)=seg %then %let _scope_seg_id=&_chall_seg_num.;
        %_chall_final_load_inputs(troncal_id=&troncal_id., seg_id=&_scope_seg_id.,
            out_train=_chall_final_train, out_oot=_chall_final_oot);
        %let _champ_mode=SINGLE_MODEL;

        proc sql outobs=1 noprint;
            select strip(Astore_Name), strip(Algo_Name)
              into :_sel_astore trimmed, :_sel_algo trimmed
            from work._chall_selected_models;
        quit;

        %_chall_final_load_model(astore_name=&_sel_astore.,
            out_table=_chall_final_rstore);

        proc astore;
            score data=PROC._chall_final_train
                out=casuser._chall_scored_train_raw
                rstore=OUT._chall_final_rstore
                copyvars=(&_copyvars.);
        run;

        proc astore;
            score data=PROC._chall_final_oot
                out=casuser._chall_scored_oot_raw
                rstore=OUT._chall_final_rstore
                copyvars=(&_copyvars.);
        run;

        data casuser._chall_scored_train(copies=0);
            length Algo_Name $32 Astore_Name $128;
            set casuser._chall_scored_train_raw;
            PD_FINAL=P_&_chall_target.1;
            Algo_Name="&_sel_algo.";
            Astore_Name="&_sel_astore.";
            drop P_&_chall_target.0 P_&_chall_target.1;
        run;

        data casuser._chall_scored_oot(copies=0);
            length Algo_Name $32 Astore_Name $128;
            set casuser._chall_scored_oot_raw;
            PD_FINAL=P_&_chall_target.1;
            Algo_Name="&_sel_algo.";
            Astore_Name="&_sel_astore.";
            drop P_&_chall_target.0 P_&_chall_target.1;
        run;
    %end;

    proc sql noprint;
        select count(*) into :_rows_train_base trimmed from PROC._chall_final_train;
        select count(*) into :_rows_train_scored trimmed from casuser._chall_scored_train;
        select count(*) into :_rows_oot_base trimmed from PROC._chall_final_oot;
        select count(*) into :_rows_oot_scored trimmed from casuser._chall_scored_oot;
    quit;

    %if &_rows_train_base. ne &_rows_train_scored. %then %put WARNING:
        [challenge_run] TRAIN scoreado no coincide con TRAIN base
        (base=&_rows_train_base. scoreado=&_rows_train_scored.).;
    %if &_rows_oot_base. ne &_rows_oot_scored. %then %put WARNING:
        [challenge_run] OOT scoreado no coincide con OOT base
        (base=&_rows_oot_base. scoreado=&_rows_oot_scored.).;

    data work._chall_input_full;
        set PROC._chall_final_train PROC._chall_final_oot;
    run;

    %_chall_build_benchmark(train_data=PROC._chall_final_train,
        oot_data=PROC._chall_final_oot, full_data=work._chall_input_full,
        target=&_chall_target., score_var=&_bmk_score_var.,
        byvar=&_chall_byvar., penalty_lambda=0.5,
        out_global=work._chall_benchmark_global,
        out_monthly=work._chall_benchmark_monthly);

    %_chall_build_global_summary(scored_train=casuser._chall_scored_train,
        scored_oot=casuser._chall_scored_oot, target=&_chall_target.,
        score_var=PD_FINAL, benchmark=work._chall_benchmark_global,
        penalty_lambda=0.5, scope=&scope., champion_mode=&_champ_mode.,
        n_selected_models=&_selected_n.,
        out=work._chall_champion_summary);

    data work._chall_scored_full;
        set casuser._chall_scored_train casuser._chall_scored_oot;
    run;

    %_chall_gini_monthly(data=work._chall_scored_full, target=&_chall_target.,
        score_var=PD_FINAL, byvar=&_chall_byvar., model_rank=1,
        model_label=Challenge Champion, algo_name=CHALLENGE,
        source=CHAMPION, out=work._chall_champion_monthly);

    data work._chall_monthly_compare;
        set work._chall_benchmark_monthly work._chall_champion_monthly;
    run;

    proc sql;
        create table work._chall_registry_marked as
        select a.*,
               case
                   when exists (
                       select 1
                       from work._chall_selected_models b
                       where upcase(a.Artifact_Prefix)=upcase(b.Artifact_Prefix)
                   ) then 1
                   else 0
               end as Is_Global_Champion
        from work._chall_registry a;
    quit;

    data work._chall_registry;
        set work._chall_registry_marked;
    run;

    libname _chalout "&_chall_tables_path.";

    data _chalout.&_chall_tbl_prefix._rgst;
        set work._chall_registry;
    run;
    data _chalout.&_chall_tbl_prefix._algc;
        set work._chall_algo_champions;
    run;
    data _chalout.&_chall_tbl_prefix._selm;
        set work._chall_selected_models;
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
        champion_data=work._chall_selected_models,
        global_data=work._chall_champion_summary,
        monthly_data=work._chall_monthly_compare,
        report_path=&_chall_report_path., images_path=&_chall_images_path.,
        file_prefix=&_chall_report_prefix., model_low=&_model_low.,
        model_high=&_model_high.);

    %_chall_final_drop_table(caslib=PROC, table=_chall_final_train);
    %_chall_final_drop_table(caslib=PROC, table=_chall_final_oot);
    %_chall_final_drop_table(caslib=OUT, table=_chall_final_rstore);
    %_chall_final_drop_table(caslib=casuser, table=_chall_scored_train_raw);
    %_chall_final_drop_table(caslib=casuser, table=_chall_scored_oot_raw);
    %_chall_final_drop_table(caslib=casuser, table=_chall_scored_train);
    %_chall_final_drop_table(caslib=casuser, table=_chall_scored_oot);

    %if &_segmented_universe.=1 %then %do;
        %do _i=1 %to &_selected_n.;
            %_chall_final_drop_table(caslib=PROC, table=_chall_seg_train_&_i.);
            %_chall_final_drop_table(caslib=PROC, table=_chall_seg_oot_&_i.);
            %_chall_final_drop_table(caslib=OUT, table=_chall_final_rstore_&_i.);
            %_chall_final_drop_table(caslib=casuser,
                table=_chall_scored_train_raw_&_i.);
            %_chall_final_drop_table(caslib=casuser,
                table=_chall_scored_oot_raw_&_i.);
            %_chall_final_drop_table(caslib=casuser,
                table=_chall_scored_train_&_i.);
            %_chall_final_drop_table(caslib=casuser,
                table=_chall_scored_oot_&_i.);
        %end;
    %end;

    proc datasets library=work nolist nowarn;
        delete _chall_registry _chall_registry_scope _chall_scope_champion
            _chall_algo_champions _chall_selected_models
            _chall_registry_marked _chall_benchmark_global
            _chall_benchmark_monthly _chall_champion_summary
            _chall_champion_monthly _chall_monthly_compare
            _chall_input_full _chall_scored_full;
    quit;

    libname _challin clear;

    %put NOTE:======================================================;
    %put NOTE: [challenge_run] FIN - &_chall_report_prefix.;
    %put NOTE:======================================================;
%mend challenge_run;
