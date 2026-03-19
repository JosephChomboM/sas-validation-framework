/* =========================================================================
decision_tree_compute.sas - Calculo base para DT Challenge (METOD9)
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

%macro _chall_dt_set_ranges(data=, num=, cat=, target=, maxdepth_cap=12,
    numbin_cap=100);
    %local _train_obs _num_vars _cat_vars _event_rate _S _P _I _C _cat_share;

    proc sql noprint;
        select count(*) into :_train_obs trimmed
        from &data.
        where _PartInd_=1;
    quit;

    %if %sysevalf(%superq(_train_obs)=, boolean) or &_train_obs. <= 0 %then %do;
        %put ERROR: [challenge_dt] No hay observaciones TRAIN para calcular
            rangos de autotune.;
        %return;
    %end;

    %let _num_vars=%sysfunc(countw(%superq(num), %str( )));
    %let _cat_vars=%sysfunc(countw(%superq(cat), %str( )));

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

    %if &_num_vars. <= 25 %then %let _P=1;
    %else %if &_num_vars. <= 60 %then %let _P=2;
    %else %let _P=3;

    %if %sysevalf(%superq(_event_rate)=, boolean) or
        %sysevalf(&_event_rate.=., boolean) %then %let _I=2;
    %else %if &_event_rate. < 0.03 or &_event_rate. > 0.97 %then %let _I=1;
    %else %if &_event_rate. < 0.10 or &_event_rate. > 0.90 %then %let _I=2;
    %else %let _I=3;

    %let _cat_share=0;
    %if &_num_vars. > 0 %then
        %let _cat_share=%sysevalf(&_cat_vars. / &_num_vars.);

    %if &_cat_share. >= 0.50 %then %let _C=1;
    %else %if &_cat_share. >= 0.20 %then %let _C=2;
    %else %let _C=3;

    %global _chall_dt_criterion_list _chall_dt_depth_lb _chall_dt_depth_init
        _chall_dt_depth_ub _chall_dt_leaf_lb _chall_dt_leaf_init
        _chall_dt_leaf_ub _chall_dt_nbin_lb _chall_dt_nbin_init
        _chall_dt_nbin_ub _chall_dt_maxdepth_bounds
        _chall_dt_minleaf_bounds _chall_dt_numbin_bounds
        _chall_dt_assignmissing_opt _chall_dt_binmethod_opt
        _chall_dt_maxbranch_opt _chall_dt_grow_opt _chall_dt_prune_opt
        _chall_dt_info;

    %let _chall_dt_criterion_list=VALUES=GINI ENTROPY IGR INIT=GINI;

    %if &_S.=1 %then %do;
        %let _chall_dt_depth_lb=3;
        %let _chall_dt_depth_init=5;
        %let _chall_dt_depth_ub=8;
    %end;
    %else %if &_S.=2 %then %do;
        %let _chall_dt_depth_lb=4;
        %let _chall_dt_depth_init=6;
        %let _chall_dt_depth_ub=9;
    %end;
    %else %do;
        %let _chall_dt_depth_lb=4;
        %let _chall_dt_depth_init=6;
        %let _chall_dt_depth_ub=10;
    %end;

    %if &_C.=1 %then
        %let _chall_dt_depth_ub=%sysfunc(min(&_chall_dt_depth_ub., 9));

    %let _chall_dt_depth_lb=%sysfunc(max(2,
        %sysfunc(min(&maxdepth_cap., &_chall_dt_depth_lb.))));
    %let _chall_dt_depth_init=%sysfunc(max(2,
        %sysfunc(min(&maxdepth_cap., &_chall_dt_depth_init.))));
    %let _chall_dt_depth_ub=%sysfunc(max(2,
        %sysfunc(min(&maxdepth_cap., &_chall_dt_depth_ub.))));
    %if &_chall_dt_depth_lb. > &_chall_dt_depth_init. %then
        %let _chall_dt_depth_lb=&_chall_dt_depth_init.;
    %if &_chall_dt_depth_init. > &_chall_dt_depth_ub. %then
        %let _chall_dt_depth_init=&_chall_dt_depth_ub.;

    %if &_S.=1 %then %do;
        %let _chall_dt_leaf_lb=5;
        %let _chall_dt_leaf_init=20;
        %let _chall_dt_leaf_ub=%sysfunc(max(50,
            %sysfunc(int(%sysevalf(0.05*&_train_obs.)))));
    %end;
    %else %if &_S.=2 %then %do;
        %let _chall_dt_leaf_lb=10;
        %let _chall_dt_leaf_init=30;
        %let _chall_dt_leaf_ub=%sysfunc(max(80,
            %sysfunc(int(%sysevalf(0.03*&_train_obs.)))));
    %end;
    %else %do;
        %let _chall_dt_leaf_lb=20;
        %let _chall_dt_leaf_init=40;
        %let _chall_dt_leaf_ub=%sysfunc(max(120,
            %sysfunc(int(%sysevalf(0.02*&_train_obs.)))));
    %end;

    %if &_I.=1 %then %do;
        %let _chall_dt_leaf_lb=%sysfunc(max(5,
            %sysfunc(floor(%sysevalf(&_chall_dt_leaf_lb./2)))));
        %let _chall_dt_leaf_init=%sysfunc(max(10,
            %sysfunc(floor(%sysevalf(&_chall_dt_leaf_init./2)))));
    %end;

    %let _chall_dt_leaf_lb=%sysfunc(max(1,
        %sysfunc(min(&_train_obs., &_chall_dt_leaf_lb.))));
    %let _chall_dt_leaf_init=%sysfunc(max(1,
        %sysfunc(min(&_train_obs., &_chall_dt_leaf_init.))));
    %let _chall_dt_leaf_ub=%sysfunc(max(1,
        %sysfunc(min(&_train_obs., &_chall_dt_leaf_ub.))));
    %if &_chall_dt_leaf_lb. > &_chall_dt_leaf_init. %then
        %let _chall_dt_leaf_lb=&_chall_dt_leaf_init.;
    %if &_chall_dt_leaf_init. > &_chall_dt_leaf_ub. %then
        %let _chall_dt_leaf_init=&_chall_dt_leaf_ub.;

    %if &_S.=1 %then %do;
        %let _chall_dt_nbin_lb=10;
        %let _chall_dt_nbin_init=20;
        %let _chall_dt_nbin_ub=40;
    %end;
    %else %if &_S.=2 %then %do;
        %let _chall_dt_nbin_lb=10;
        %let _chall_dt_nbin_init=25;
        %let _chall_dt_nbin_ub=60;
    %end;
    %else %do;
        %let _chall_dt_nbin_lb=10;
        %let _chall_dt_nbin_init=30;
        %let _chall_dt_nbin_ub=80;
    %end;

    %if &_C.=1 %then
        %let _chall_dt_nbin_ub=%sysfunc(min(&_chall_dt_nbin_ub., 60));
    %let _chall_dt_nbin_ub=%sysfunc(min(&numbin_cap., &_chall_dt_nbin_ub.));
    %if &_chall_dt_nbin_lb. > &_chall_dt_nbin_init. %then
        %let _chall_dt_nbin_lb=&_chall_dt_nbin_init.;
    %if &_chall_dt_nbin_init. > &_chall_dt_nbin_ub. %then
        %let _chall_dt_nbin_init=&_chall_dt_nbin_ub.;

    %let _chall_dt_maxdepth_bounds=LB=&_chall_dt_depth_lb.
        INIT=&_chall_dt_depth_init. UB=&_chall_dt_depth_ub.;
    %let _chall_dt_minleaf_bounds=LB=&_chall_dt_leaf_lb.
        INIT=&_chall_dt_leaf_init. UB=&_chall_dt_leaf_ub.;
    %let _chall_dt_numbin_bounds=LB=&_chall_dt_nbin_lb.
        INIT=&_chall_dt_nbin_init. UB=&_chall_dt_nbin_ub.;

    %let _chall_dt_assignmissing_opt=USEINSEARCH;
    %let _chall_dt_binmethod_opt=QUANTILE;
    %let _chall_dt_maxbranch_opt=2;
    %let _chall_dt_grow_opt=GINI;
    %let _chall_dt_prune_opt=COSTCOMPLEXITY;

    %let _chall_dt_info=tabla=&data. nobs=&_train_obs. num_inputs=&_num_vars.
        cat_inputs=&_cat_vars. event_rate=&_event_rate. S=&_S. P=&_P.
        I=&_I. C=&_C.;

    %put NOTE: [challenge_dt] &_chall_dt_info.;
    %put NOTE: [challenge_dt] CRITERION=&_chall_dt_criterion_list.;
    %put NOTE: [challenge_dt] DEPTH=&_chall_dt_maxdepth_bounds.;
    %put NOTE: [challenge_dt] LEAF=&_chall_dt_minleaf_bounds.;
    %put NOTE: [challenge_dt] NBIN=&_chall_dt_numbin_bounds.;
%mend _chall_dt_set_ranges;

%macro _chall_dt_tune(data=casuser._chall_train_part, num_input=, cat_input=,
    target_input=, nparallel=5, eval_out=work._chall_eval_history,
    best_out=work._chall_bestconfiguration);

    ods exclude all;
    proc treesplit data=&data.
        assignmissing=&_chall_dt_assignmissing_opt.
        binmethod=&_chall_dt_binmethod_opt.
        maxbranch=&_chall_dt_maxbranch_opt.
        seed=12345;
        prune &_chall_dt_prune_opt.;
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
                criterion(&_chall_dt_criterion_list.)
                maxdepth(&_chall_dt_maxdepth_bounds.)
                minleafsize(&_chall_dt_minleaf_bounds.)
                numbin(&_chall_dt_numbin_bounds.)
            );
        ods output BestConfiguration=&best_out.;
        ods output EvaluationHistory=&eval_out.;
    run;
    ods exclude none;

    data &eval_out.;
        length Crit $16 Missing $12 BinMethod $16 Prune $24;
        set &eval_out.;
        if missing(MAXLEVEL) and not missing(MAXDEPTH) then
            MAXLEVEL=MAXDEPTH;
        if missing(NBINS) and not missing(NUMBIN) then NBINS=NUMBIN;
        if missing(LEAFSIZE) and not missing(MINLEAFSIZE) then
            LEAFSIZE=MINLEAFSIZE;
        if missing(CRIT) and not missing(CRITERION) then CRIT=CRITERION;
        if missing(CRIT) then CRIT="&_chall_dt_grow_opt.";
        Missing="&_chall_dt_assignmissing_opt.";
        BinMethod="&_chall_dt_binmethod_opt.";
        MaxBranch=&_chall_dt_maxbranch_opt.;
        Prune="&_chall_dt_prune_opt.";
    run;
%mend _chall_dt_tune;

%macro _chall_prepare_topk(eval_data=work._chall_eval_history, top_k=40,
    out=work._chall_topk_cfg);

    proc sort data=&eval_data. out=work._chall_hist_sorted;
        by descending GiniCoefficient;
    run;

    data &out.;
        length Algo_Name $32 Crit $16 Missing $12 BinMethod $16 Prune $24;
        set work._chall_hist_sorted(obs=&top_k.);
        cfg_id=_n_;
        model_rank=_n_;
        Algo_Name="Decision Tree";
        if missing(CRIT) then CRIT="&_chall_dt_grow_opt.";
        Missing="&_chall_dt_assignmissing_opt.";
        BinMethod="&_chall_dt_binmethod_opt.";
        MaxBranch=&_chall_dt_maxbranch_opt.;
        Prune="&_chall_dt_prune_opt.";
    run;

    proc datasets library=work nolist nowarn;
        delete _chall_hist_sorted;
    quit;
%mend _chall_prepare_topk;
