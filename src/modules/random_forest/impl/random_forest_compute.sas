/* =========================================================================
random_forest_compute.sas - Calculo base para RF Challenge (METOD9)
========================================================================= */

%macro _chall_gini(data=, target=, score_var=, outmac=);
    %global &outmac.;
    %let &outmac.=.;

    proc freqtab data=&data. noprint missing;
        tables &target. * &score_var. / measures;
        output out=work._chall_gini_freqtab smdcr;
    run;

    data _null_;
        set work._chall_gini_freqtab(obs=1);
        call symputx("&outmac.", abs(_smdcr_), "G");
    run;

    proc datasets library=work nolist nowarn;
        delete _chall_gini_freqtab;
    quit;
%mend _chall_gini;

%macro _chall_gini_monthly(data=, target=, score_var=, byvar=, model_rank=0,
    model_label=Benchmark Base, algo_name=BASELINE, source=BENCHMARK,
    out=work._chall_monthly_out);

    proc sort data=&data. out=work._chall_monthly_src;
        by &byvar.;
    run;

    proc freqtab data=work._chall_monthly_src noprint missing;
        by &byvar.;
        tables &target. * &score_var. / measures;
        output out=work._chall_monthly_freq smdcr;
    run;

    proc sql;
        create table work._chall_monthly_counts as
        select &byvar. as Periodo,
               count(*) as N_Total,
               sum(&target.) as N_Default
        from work._chall_monthly_src
        group by &byvar.;
    quit;

    proc sql;
        create table &out. as
        select a.&byvar. as Periodo,
               b.N_Total,
               b.N_Default,
               abs(a._smdcr_) as Gini format=8.4
        from work._chall_monthly_freq a
        left join work._chall_monthly_counts b
          on a.&byvar. = b.Periodo
        order by Periodo;
    quit;

    data &out.;
        length Model_Label $64 Algo_Name $32 Source $16;
        set &out.;
        Model_Rank=&model_rank.;
        Model_Label="&model_label.";
        Algo_Name="&algo_name.";
        Source="&source.";
    run;

    proc datasets library=work nolist nowarn;
        delete _chall_monthly_src _chall_monthly_freq _chall_monthly_counts;
    quit;
%mend _chall_gini_monthly;

%macro _chall_build_benchmark(train_data=, oot_data=, full_data=, target=,
    score_var=, byvar=, penalty_lambda=0.5,
    out_global=work._chall_benchmark_global,
    out_monthly=work._chall_benchmark_monthly);

    %local _bmk_g_penal;

    %_chall_gini(data=&train_data., target=&target., score_var=&score_var.,
        outmac=_chall_bmk_g_train);
    %_chall_gini(data=&oot_data., target=&target., score_var=&score_var.,
        outmac=_chall_bmk_g_oot);
    %let _bmk_g_penal=%sysevalf(&_chall_bmk_g_oot. -
        %sysevalf(&penalty_lambda. * %sysevalf(&_chall_bmk_g_train. - &_chall_bmk_g_oot.)));

    data &out_global.;
        length Dataset $8 Score_Source $64;
        Dataset="TRAIN";
        Score_Source="&score_var.";
        Gini=&_chall_bmk_g_train.;
        Gini_Penalizado=&_bmk_g_penal.;
        output;
        Dataset="OOT";
        Score_Source="&score_var.";
        Gini=&_chall_bmk_g_oot.;
        Gini_Penalizado=&_bmk_g_penal.;
        output;
        format Gini Gini_Penalizado 8.4;
    run;

    %_chall_gini_monthly(data=&full_data., target=&target., score_var=&score_var.,
        byvar=&byvar., model_rank=0, model_label=Benchmark Base,
        algo_name=BASELINE, source=BENCHMARK, out=&out_monthly.);
%mend _chall_build_benchmark;

%macro _chall_rf_set_ranges(data=, num=, cat=, target=, ntrees_cap=350,
    maxdepth_cap=30);
    %local _train_obs _num_vars _cat_vars _total_inputs _sqrtp
        _event_rate _S _P _I _C _cat_share;

    proc sql noprint;
        select count(*) into :_train_obs trimmed
        from &data.
        where _PartInd_=1;
    quit;

    %if %sysevalf(%superq(_train_obs)=, boolean) or &_train_obs. <= 0 %then %do;
        %put ERROR: [challenge_rf] No hay observaciones TRAIN para calcular
            rangos de autotune.;
        %return;
    %end;

    %let _num_vars=%sysfunc(countw(%superq(num), %str( )));
    %let _cat_vars=%sysfunc(countw(%superq(cat), %str( )));
    %let _total_inputs=%sysfunc(countw(%sysfunc(compbl(&num. &cat.)), %str( )));
    %if &_total_inputs. < 1 %then %let _total_inputs=1;

    %let _event_rate=.;
    %if %length(%superq(target)) > 0 %then %do;
        proc sql noprint;
            select mean((&target.=1)*1) into :_event_rate trimmed
            from &data.
            where _PartInd_=1;
        quit;
    %end;

    %if &_train_obs. <= 20000 %then %let _S=1;
    %else %if &_train_obs. <= 100000 %then %let _S=2;
    %else %if &_train_obs. <= 300000 %then %let _S=3;
    %else %let _S=4;

    %if &_total_inputs. <= 20 %then %let _P=1;
    %else %if &_total_inputs. <= 60 %then %let _P=2;
    %else %let _P=3;

    %if %sysevalf(%superq(_event_rate)=, boolean) or
        %sysevalf(&_event_rate.=., boolean) %then %let _I=2;
    %else %if &_event_rate. < 0.03 or &_event_rate. > 0.97 %then %let _I=1;
    %else %if &_event_rate. < 0.10 or &_event_rate. > 0.90 %then %let _I=2;
    %else %let _I=3;

    %let _cat_share=0;
    %if &_total_inputs. > 0 %then
        %let _cat_share=%sysevalf(&_cat_vars. / &_total_inputs.);

    %if &_cat_share. >= 0.50 %then %let _C=1;
    %else %if &_cat_share. >= 0.20 %then %let _C=2;
    %else %let _C=3;

    %let _sqrtp=%sysfunc(max(1,
        %sysfunc(floor(%sysevalf(%sysfunc(sqrt(&_total_inputs.)))))));

    %global _chall_rf_INBAG_LB _chall_rf_INBAG_INIT _chall_rf_INBAG_UB
        _chall_rf_DEPTH_LB _chall_rf_DEPTH_INIT _chall_rf_DEPTH_UB
        _chall_rf_LEAF_LB _chall_rf_LEAF_INIT _chall_rf_LEAF_UB
        _chall_rf_NBIN_LB _chall_rf_NBIN_INIT _chall_rf_NBIN_UB
        _chall_rf_NTREES_LB _chall_rf_NTREES_INIT _chall_rf_NTREES_UB
        _chall_rf_MTRY_LB _chall_rf_MTRY_INIT _chall_rf_MTRY_UB;

    %if &_S.=1 %then %do;
        %let _chall_rf_INBAG_LB=0.70;
        %let _chall_rf_INBAG_INIT=0.80;
        %let _chall_rf_INBAG_UB=0.90;
        %let _chall_rf_DEPTH_LB=6;
        %let _chall_rf_DEPTH_INIT=12;
        %let _chall_rf_DEPTH_UB=16;
        %let _chall_rf_LEAF_LB=1;
        %let _chall_rf_LEAF_INIT=5;
        %let _chall_rf_LEAF_UB=30;
        %let _chall_rf_NBIN_LB=50;
        %let _chall_rf_NBIN_INIT=100;
        %let _chall_rf_NBIN_UB=150;
        %let _chall_rf_NTREES_LB=80;
        %let _chall_rf_NTREES_INIT=120;
        %let _chall_rf_NTREES_UB=180;
        %let _chall_rf_MTRY_LB=%sysfunc(max(1,
            %sysfunc(floor(%sysevalf(0.5*&_sqrtp.)))));
        %let _chall_rf_MTRY_INIT=&_sqrtp.;
        %let _chall_rf_MTRY_UB=%sysfunc(min(&_total_inputs.,
            %sysfunc(floor(%sysevalf(1.5*&_sqrtp.)))));
    %end;
    %else %if &_S.=2 %then %do;
        %let _chall_rf_INBAG_LB=0.60;
        %let _chall_rf_INBAG_INIT=0.70;
        %let _chall_rf_INBAG_UB=0.85;
        %let _chall_rf_DEPTH_LB=8;
        %let _chall_rf_DEPTH_INIT=18;
        %let _chall_rf_DEPTH_UB=24;
        %let _chall_rf_LEAF_LB=5;
        %let _chall_rf_LEAF_INIT=20;
        %let _chall_rf_LEAF_UB=60;
        %let _chall_rf_NBIN_LB=50;
        %let _chall_rf_NBIN_INIT=100;
        %let _chall_rf_NBIN_UB=200;
        %let _chall_rf_NTREES_LB=100;
        %let _chall_rf_NTREES_INIT=150;
        %let _chall_rf_NTREES_UB=250;
        %let _chall_rf_MTRY_LB=%sysfunc(max(1,
            %sysfunc(floor(%sysevalf(0.5*&_sqrtp.)))));
        %let _chall_rf_MTRY_INIT=&_sqrtp.;
        %let _chall_rf_MTRY_UB=%sysfunc(min(&_total_inputs.,
            %sysfunc(floor(%sysevalf(2*&_sqrtp.)))));
    %end;
    %else %if &_S.=3 %then %do;
        %let _chall_rf_INBAG_LB=0.50;
        %let _chall_rf_INBAG_INIT=0.60;
        %let _chall_rf_INBAG_UB=0.70;
        %let _chall_rf_DEPTH_LB=10;
        %let _chall_rf_DEPTH_INIT=20;
        %let _chall_rf_DEPTH_UB=28;
        %let _chall_rf_LEAF_LB=10;
        %let _chall_rf_LEAF_INIT=50;
        %let _chall_rf_LEAF_UB=100;
        %let _chall_rf_NBIN_LB=40;
        %let _chall_rf_NBIN_INIT=80;
        %let _chall_rf_NBIN_UB=120;
        %let _chall_rf_NTREES_LB=150;
        %let _chall_rf_NTREES_INIT=200;
        %let _chall_rf_NTREES_UB=300;
        %let _chall_rf_MTRY_LB=%sysfunc(max(1,
            %sysfunc(floor(%sysevalf(0.5*&_sqrtp.)))));
        %let _chall_rf_MTRY_INIT=%sysfunc(max(1,
            %sysfunc(floor(%sysevalf(0.8*&_sqrtp.)))));
        %let _chall_rf_MTRY_UB=%sysfunc(min(&_total_inputs.,
            %sysfunc(floor(%sysevalf(1.5*&_sqrtp.)))));
    %end;
    %else %do;
        %let _chall_rf_INBAG_LB=0.50;
        %let _chall_rf_INBAG_INIT=0.60;
        %let _chall_rf_INBAG_UB=0.65;
        %let _chall_rf_DEPTH_LB=10;
        %let _chall_rf_DEPTH_INIT=18;
        %let _chall_rf_DEPTH_UB=24;
        %let _chall_rf_LEAF_LB=20;
        %let _chall_rf_LEAF_INIT=60;
        %let _chall_rf_LEAF_UB=120;
        %let _chall_rf_NBIN_LB=30;
        %let _chall_rf_NBIN_INIT=60;
        %let _chall_rf_NBIN_UB=100;
        %let _chall_rf_NTREES_LB=200;
        %let _chall_rf_NTREES_INIT=250;
        %let _chall_rf_NTREES_UB=350;
        %let _chall_rf_MTRY_LB=%sysfunc(max(1,
            %sysfunc(floor(%sysevalf(0.5*&_sqrtp.)))));
        %let _chall_rf_MTRY_INIT=%sysfunc(max(1,
            %sysfunc(floor(%sysevalf(0.8*&_sqrtp.)))));
        %let _chall_rf_MTRY_UB=%sysfunc(min(&_total_inputs.,
            %sysfunc(floor(%sysevalf(1.5*&_sqrtp.)))));
    %end;

    %if &_I.=1 %then %do;
        %let _chall_rf_LEAF_LB=%sysfunc(max(1,
            %sysfunc(floor(%sysevalf(&_chall_rf_LEAF_LB./2)))));
        %let _chall_rf_LEAF_INIT=%sysfunc(max(1,
            %sysfunc(floor(%sysevalf(&_chall_rf_LEAF_INIT./2)))));
        %let _chall_rf_NTREES_UB=%sysfunc(min(&ntrees_cap.,
            %eval(&_chall_rf_NTREES_UB. + 50)));
    %end;

    %if &_C.=1 %then %do;
        %let _chall_rf_LEAF_LB=%sysfunc(max(&_chall_rf_LEAF_LB., 5));
        %let _chall_rf_DEPTH_UB=%sysfunc(min(&_chall_rf_DEPTH_UB.,
            %eval(&maxdepth_cap. - 2)));
    %end;

    %if &_P.=3 %then %do;
        %let _chall_rf_MTRY_LB=%sysfunc(max(1,
            %sysfunc(floor(%sysevalf(0.4*&_sqrtp.)))));
        %let _chall_rf_MTRY_INIT=%sysfunc(max(1,
            %sysfunc(floor(%sysevalf(0.7*&_sqrtp.)))));
    %end;

    %macro _chall_rf_clamp3(base);
        %if &&&base._LB. > &&&base._INIT. %then %let &base._LB=&&&base._INIT.;
        %if &&&base._INIT. > &&&base._UB. %then %let &base._INIT=&&&base._UB.;
        %if &&&base._LB. > &&&base._UB. %then %let &base._LB=&&&base._UB.;
    %mend _chall_rf_clamp3;

    %if &_chall_rf_INBAG_LB. < 0.3 %then %let _chall_rf_INBAG_LB=0.3;
    %if &_chall_rf_INBAG_UB. > 1.0 %then %let _chall_rf_INBAG_UB=1.0;
    %if &_chall_rf_INBAG_INIT. < 0.3 %then %let _chall_rf_INBAG_INIT=0.3;
    %if &_chall_rf_INBAG_INIT. > &_chall_rf_INBAG_UB. %then
        %let _chall_rf_INBAG_INIT=&_chall_rf_INBAG_UB.;
    %if &_chall_rf_INBAG_LB. > &_chall_rf_INBAG_INIT. %then
        %let _chall_rf_INBAG_LB=&_chall_rf_INBAG_INIT.;

    %let _chall_rf_DEPTH_LB=%sysfunc(max(2,
        %sysfunc(min(&maxdepth_cap., &_chall_rf_DEPTH_LB.))));
    %let _chall_rf_DEPTH_INIT=%sysfunc(max(2,
        %sysfunc(min(&maxdepth_cap., &_chall_rf_DEPTH_INIT.))));
    %let _chall_rf_DEPTH_UB=%sysfunc(max(2,
        %sysfunc(min(&maxdepth_cap., &_chall_rf_DEPTH_UB.))));
    %_chall_rf_clamp3(_chall_rf_DEPTH);

    %let _chall_rf_LEAF_LB=%sysfunc(max(1,
        %sysfunc(min(&_train_obs., &_chall_rf_LEAF_LB.))));
    %let _chall_rf_LEAF_INIT=%sysfunc(max(1,
        %sysfunc(min(&_train_obs., &_chall_rf_LEAF_INIT.))));
    %let _chall_rf_LEAF_UB=%sysfunc(max(1,
        %sysfunc(min(&_train_obs., &_chall_rf_LEAF_UB.))));
    %_chall_rf_clamp3(_chall_rf_LEAF);

    %let _chall_rf_NBIN_LB=%sysfunc(max(10,
        %sysfunc(min(256, &_chall_rf_NBIN_LB.))));
    %let _chall_rf_NBIN_INIT=%sysfunc(max(10,
        %sysfunc(min(256, &_chall_rf_NBIN_INIT.))));
    %let _chall_rf_NBIN_UB=%sysfunc(max(10,
        %sysfunc(min(256, &_chall_rf_NBIN_UB.))));
    %_chall_rf_clamp3(_chall_rf_NBIN);

    %let _chall_rf_NTREES_LB=%sysfunc(max(50,
        %sysfunc(min(&ntrees_cap., &_chall_rf_NTREES_LB.))));
    %let _chall_rf_NTREES_INIT=%sysfunc(max(50,
        %sysfunc(min(&ntrees_cap., &_chall_rf_NTREES_INIT.))));
    %let _chall_rf_NTREES_UB=%sysfunc(max(50,
        %sysfunc(min(&ntrees_cap., &_chall_rf_NTREES_UB.))));
    %_chall_rf_clamp3(_chall_rf_NTREES);

    %let _chall_rf_MTRY_LB=%sysfunc(max(1,
        %sysfunc(min(&_total_inputs., &_chall_rf_MTRY_LB.))));
    %let _chall_rf_MTRY_INIT=%sysfunc(max(1,
        %sysfunc(min(&_total_inputs., &_chall_rf_MTRY_INIT.))));
    %let _chall_rf_MTRY_UB=%sysfunc(max(1,
        %sysfunc(min(&_total_inputs., &_chall_rf_MTRY_UB.))));
    %_chall_rf_clamp3(_chall_rf_MTRY);

    %global _chall_rf_inbag_bounds _chall_rf_maxdepth_bounds
        _chall_rf_minleaf_bounds _chall_rf_numbin_bounds
        _chall_rf_ntrees_bounds _chall_rf_varstry_bounds
        _chall_rf_assignmissing_opt _chall_rf_binmethod_opt
        _chall_rf_maxbranch_opt _chall_rf_grow_opt _chall_rf_info;

    %let _chall_rf_inbag_bounds=LB=&_chall_rf_INBAG_LB.
        INIT=&_chall_rf_INBAG_INIT. UB=&_chall_rf_INBAG_UB.;
    %let _chall_rf_maxdepth_bounds=LB=&_chall_rf_DEPTH_LB.
        INIT=&_chall_rf_DEPTH_INIT. UB=&_chall_rf_DEPTH_UB.;
    %let _chall_rf_minleaf_bounds=LB=&_chall_rf_LEAF_LB.
        INIT=&_chall_rf_LEAF_INIT. UB=&_chall_rf_LEAF_UB.;
    %let _chall_rf_numbin_bounds=LB=&_chall_rf_NBIN_LB.
        INIT=&_chall_rf_NBIN_INIT. UB=&_chall_rf_NBIN_UB.;
    %let _chall_rf_ntrees_bounds=LB=&_chall_rf_NTREES_LB.
        INIT=&_chall_rf_NTREES_INIT. UB=&_chall_rf_NTREES_UB.;
    %let _chall_rf_varstry_bounds=LB=&_chall_rf_MTRY_LB.
        INIT=&_chall_rf_MTRY_INIT. UB=&_chall_rf_MTRY_UB.;

    %let _chall_rf_assignmissing_opt=USEINSEARCH;
    %let _chall_rf_binmethod_opt=QUANTILE;
    %let _chall_rf_maxbranch_opt=2;
    %let _chall_rf_grow_opt=GINI;

    %let _chall_rf_info=tabla=&data. nobs=&_train_obs. total_inputs=&_total_inputs.
        num_inputs=&_num_vars. cat_inputs=&_cat_vars. event_rate=&_event_rate.
        S=&_S. P=&_P. I=&_I. C=&_C.;

    %put NOTE: [challenge_rf] &_chall_rf_info.;
    %put NOTE: [challenge_rf] INBAG=&_chall_rf_inbag_bounds.;
    %put NOTE: [challenge_rf] DEPTH=&_chall_rf_maxdepth_bounds.;
    %put NOTE: [challenge_rf] LEAF=&_chall_rf_minleaf_bounds.;
    %put NOTE: [challenge_rf] NBIN=&_chall_rf_numbin_bounds.;
    %put NOTE: [challenge_rf] NTREES=&_chall_rf_ntrees_bounds.;
    %put NOTE: [challenge_rf] MTRY=&_chall_rf_varstry_bounds.;
%mend _chall_rf_set_ranges;

%macro _chall_rf_tune(data=casuser._chall_train_part, num_input=, cat_input=,
    target_input=, nparallel=5, eval_out=work._chall_eval_history,
    best_out=work._chall_bestconfiguration);

    ods exclude all;
    proc forest data=&data.
        assignmissing=&_chall_rf_assignmissing_opt.
        binmethod=&_chall_rf_binmethod_opt.
        maxbranch=&_chall_rf_maxbranch_opt.
        seed=12345;
        grow &_chall_rf_grow_opt.;
        partition rolevar=_PartInd_(train='1' validate='0' test='2');
        %if %length(%superq(num_input)) > 0 %then %do;
            input &num_input. / level=interval;
        %end;
        %if %length(%superq(cat_input)) > 0 %then %do;
            input &cat_input. / level=nominal;
        %end;
        target &target_input. / level=nominal;
        autotune
            historytable=casuser._chall_evalhistory_cas
            evalhistory=all
            targetevent="1"
            objective=gini
            searchmethod=BAYESIAN
            nparallel=&nparallel.
            useparameters=custom
            tuningparameters=(
                inbagfraction(&_chall_rf_inbag_bounds.)
                maxdepth(&_chall_rf_maxdepth_bounds.)
                minleafsize(&_chall_rf_minleaf_bounds.)
                numbin(&_chall_rf_numbin_bounds.)
                ntrees(&_chall_rf_ntrees_bounds.)
                vars_to_try(&_chall_rf_varstry_bounds.)
            );
        ods output BestConfiguration=&best_out.;
        ods output EvaluationHistory=&eval_out.;
    run;
    ods exclude none;

    data &eval_out.;
        set &eval_out.;
        if missing(NTREE) and not missing(NTREES) then NTREE=NTREES;
        if missing(M) and not missing(VARS_TO_TRY) then M=VARS_TO_TRY;
        if missing(BOOTSTRAP) and not missing(INBAGFRACTION) then
            BOOTSTRAP=INBAGFRACTION;
        if missing(MAXLEVEL) and not missing(MAXDEPTH) then
            MAXLEVEL=MAXDEPTH;
        if missing(NBINS) and not missing(NUMBIN) then NBINS=NUMBIN;
        if missing(LEAFSIZE) and not missing(MINLEAFSIZE) then
            LEAFSIZE=MINLEAFSIZE;
    run;
%mend _chall_rf_tune;

%macro _chall_prepare_topk(eval_data=work._chall_eval_history, top_k=40,
    out=work._chall_topk_cfg);

    proc sort data=&eval_data. out=work._chall_hist_sorted;
        by descending GiniCoefficient;
    run;

    data &out.;
        length Algo_Name $32 Missing $12 BinMethod $16 Grow $16;
        set work._chall_hist_sorted(obs=&top_k.);
        cfg_id=_n_;
        model_rank=_n_;
        Algo_Name="Random Forest";
        Missing="&_chall_rf_assignmissing_opt.";
        BinMethod="&_chall_rf_binmethod_opt.";
        MaxBranch=&_chall_rf_maxbranch_opt.;
        Grow="&_chall_rf_grow_opt.";
    run;

    proc datasets library=work nolist nowarn;
        delete _chall_hist_sorted;
    quit;
%mend _chall_prepare_topk;
