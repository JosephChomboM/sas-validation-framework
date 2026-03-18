/* =========================================================================
challenge_prechallenge.sas - Preparacion de inputs para METOD9 Challenge
========================================================================= */

%macro _chall_push_unique(list_name=, value=);
    %local _current _exists _i _token;
    %let _exists=0;

    %if %length(%superq(value))=0 %then %return;

    %let _current=&&&list_name.;
    %let _i=1;
    %let _token=%scan(%superq(_current), &_i., %str( ));

    %do %while(%length(%superq(_token)) > 0);
        %if %upcase(%superq(_token))=%upcase(%superq(value)) %then %let _exists=1;
        %let _i=%eval(&_i. + 1);
        %let _token=%scan(%superq(_current), &_i., %str( ));
    %end;

    %if &_exists.=0 %then %do;
        %if %length(%superq(_current))=0 %then %let &list_name.=%superq(value);
        %else %let &list_name.=&&&list_name. %superq(value);
    %end;
%mend _chall_push_unique;

%macro _chall_var_exists(data=, var=, outvar=_chall_exists);
    %local _dsid _rc _exists;
    %let _exists=0;
    %let _dsid=%sysfunc(open(&data.));
    %if &_dsid. > 0 %then %do;
        %if %sysfunc(varnum(&_dsid., &var.)) > 0 %then %let _exists=1;
        %let _rc=%sysfunc(close(&_dsid.));
    %end;
    %let &outvar=&_exists.;
%mend _chall_var_exists;

%macro _chall_intersect_inputs(train_data=, oot_data=, vars_num=, vars_cat=,
    id_var=, var_seg=, out_num=_chall_vars_num_final,
    out_cat=_chall_vars_cat_final, out_id_var=_chall_id_var_final,
    out_seg_var=_chall_seg_var_final);

    %local _num_final _cat_final _idx _var _has_train _has_oot _id_final
        _seg_final;

    %let _num_final=;
    %let _cat_final=;
    %let _id_final=;
    %let _seg_final=;

    %let _idx=1;
    %let _var=%scan(%superq(vars_num), &_idx., %str( ));
    %do %while(%length(%superq(_var)) > 0);
        %_chall_var_exists(data=&train_data., var=&_var., outvar=_has_train);
        %_chall_var_exists(data=&oot_data., var=&_var., outvar=_has_oot);
        %if &_has_train.=1 and &_has_oot.=1 %then %do;
            %_chall_push_unique(list_name=_num_final, value=&_var.);
        %end;
        %else %do;
            %put WARNING: [challenge_prechallenge] Variable numerica &_var.
                no esta disponible en TRAIN y OOT al mismo tiempo. Se omite.;
        %end;
        %let _idx=%eval(&_idx. + 1);
        %let _var=%scan(%superq(vars_num), &_idx., %str( ));
    %end;

    %let _idx=1;
    %let _var=%scan(%superq(vars_cat), &_idx., %str( ));
    %do %while(%length(%superq(_var)) > 0);
        %_chall_var_exists(data=&train_data., var=&_var., outvar=_has_train);
        %_chall_var_exists(data=&oot_data., var=&_var., outvar=_has_oot);
        %if &_has_train.=1 and &_has_oot.=1 %then %do;
            %_chall_push_unique(list_name=_cat_final, value=&_var.);
        %end;
        %else %do;
            %put WARNING: [challenge_prechallenge] Variable categorica &_var.
                no esta disponible en TRAIN y OOT al mismo tiempo. Se omite.;
        %end;
        %let _idx=%eval(&_idx. + 1);
        %let _var=%scan(%superq(vars_cat), &_idx., %str( ));
    %end;

    %if %length(%superq(id_var)) > 0 %then %do;
        %_chall_var_exists(data=&train_data., var=&id_var., outvar=_has_train);
        %_chall_var_exists(data=&oot_data., var=&id_var., outvar=_has_oot);
        %if &_has_train.=1 and &_has_oot.=1 %then %let _id_final=&id_var.;
        %else %put WARNING: [challenge_prechallenge] id_var=&id_var. no se
            cargara porque no esta disponible en ambos splits.;
    %end;

    %if %length(%superq(var_seg)) > 0 %then %do;
        %_chall_var_exists(data=&train_data., var=&var_seg., outvar=_has_train);
        %_chall_var_exists(data=&oot_data., var=&var_seg., outvar=_has_oot);
        %if &_has_train.=1 and &_has_oot.=1 %then %let _seg_final=&var_seg.;
        %else %put WARNING: [challenge_prechallenge] var_seg=&var_seg. no se
            cargara porque no esta disponible en ambos splits.;
    %end;

    %let &out_num=%sysfunc(compbl(&_num_final.));
    %let &out_cat=%sysfunc(compbl(&_cat_final.));
    %let &out_id_var=&_id_final.;
    %let &out_seg_var=&_seg_final.;
%mend _chall_intersect_inputs;

%macro _chall_prepare_inputs(input_caslib=PROC, train_table=_train_input,
    oot_table=_oot_input, keep_train=, keep_oot=,
    out_train=casuser._chall_train_src, out_oot=casuser._chall_oot_src);

    proc fedsql sessref=conn;
        create table &out_train. {options replace=true} as
            select &keep_train.
            from &input_caslib..&train_table.;
    quit;

    proc fedsql sessref=conn;
        create table &out_oot. {options replace=true} as
            select &keep_oot.
            from &input_caslib..&oot_table.;
    quit;
%mend _chall_prepare_inputs;

%macro _chall_presample_work(input_data=, target=, byvar=, enabled=AUTO,
    max_cells=25000000, seed=12345, out_data=work._chall_train_raw,
    out_total_cells=_chall_total_cells, out_sampling_ratio=_chall_sampling_ratio,
    out_sampled_flag=_chall_sampled_flag);

    %local _dsid _nobs _nvars _rc _total_cells _ratio _mode;

    %let _mode=%upcase(%superq(enabled));
    %if %length(%superq(_mode))=0 %then %let _mode=AUTO;

    %let _dsid=%sysfunc(open(&input_data.));
    %if &_dsid. <= 0 %then %do;
        %let &out_total_cells=0;
        %let &out_sampling_ratio=1;
        %let &out_sampled_flag=0;
        %put ERROR: [challenge_prechallenge] No se pudo abrir &input_data.
            para presampling.;
        %return;
    %end;

    %let _nobs=%sysfunc(attrn(&_dsid., NOBS));
    %let _nvars=%sysfunc(attrn(&_dsid., NVARS));
    %let _rc=%sysfunc(close(&_dsid.));

    %let _total_cells=%sysevalf(&_nobs. * &_nvars.);
    %let _ratio=1;

    %if %sysevalf(%superq(max_cells)=, boolean) %then %let max_cells=25000000;
    %if %sysevalf(&_total_cells. > &max_cells.) and &_mode. ne 0 %then %do;
        %let _ratio=%sysevalf(&max_cells. / &_total_cells.);
        %if %sysevalf(&_ratio. > 1) %then %let _ratio=1;

        proc surveyselect data=&input_data. method=srs samprate=&_ratio.
            seed=&seed. out=&out_data.;
            strata &byvar. &target.;
        run;

        %let &out_sampled_flag=1;
        %put NOTE: [challenge_prechallenge] Presampling activado.
            total_cells=&_total_cells. ratio=&_ratio..;
    %end;
    %else %do;
        data &out_data.;
            set &input_data.;
        run;
        %let &out_sampled_flag=0;
        %put NOTE: [challenge_prechallenge] Presampling no aplicado.
            total_cells=&_total_cells..;
    %end;

    %let &out_total_cells=&_total_cells.;
    %let &out_sampling_ratio=&_ratio.;
%mend _chall_presample_work;

%macro _chall_build_partition(train_data=, oot_data=, target=, byvar=,
    partition_pct=70, seed=12345, out_train_part=work._chall_train_part,
    out_train=work._chall_train, out_valid=work._chall_valid,
    out_testoot=work._chall_testoot, out_full=work._chall_full_data);

    proc sort data=&train_data. out=work._chall_train_for_part;
        by &byvar. &target.;
    run;

    proc partition data=work._chall_train_for_part partind seed=&seed.
        samppct=&partition_pct.;
        by &byvar. &target.;
        output out=&out_train_part.;
    run;

    data &out_train.;
        set &out_train_part.(where=(_PartInd_=1));
    run;

    data &out_valid.;
        set &out_train_part.(where=(_PartInd_=0));
    run;

    data &out_testoot.;
        set &oot_data.;
        _PartInd_=2;
    run;

    data &out_full.;
        set &out_train_part. &out_testoot.;
    run;
%mend _chall_build_partition;

%macro _chall_publish_work(work_data=, cas_table=);
    data casuser.&cas_table.;
        set &work_data.;
    run;
%mend _chall_publish_work;
