/* =========================================================================
gradient_boosting_compute.sas - Calculo base para GB Challenge (METOD9)
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

%macro _chall_gb_set_ranges(data=, num=, cat=, ntrees_cap=800);
    %local total_inputs p_inputs S P;

    proc sql noprint;
        select count(*) into :_chall_train_obs trimmed
        from &data.
        where _PartInd_=1;
    quit;

    %if %sysevalf(%superq(_chall_train_obs)=, boolean) or &_chall_train_obs. <= 0 %then %do;
        %put ERROR: [challenge_gb] No hay observaciones TRAIN para calcular
            rangos de autotune.;
        %return;
    %end;

    %let total_inputs=&num. &cat.;
    %let p_inputs=%sysfunc(countw(&total_inputs., %str( )));

    %if &_chall_train_obs. <= 30000 %then %let S=1;
    %else %if &_chall_train_obs. <= 150000 %then %let S=2;
    %else %if &_chall_train_obs. <= 500000 %then %let S=3;
    %else %let S=4;

    %if &p_inputs. <= 20 %then %let P=1;
    %else %if &p_inputs. <= 60 %then %let P=2;
    %else %let P=3;

    %macro _chall_base(param, s1, s2, s3, s4);
        %global _chall_&param._LB _chall_&param._INIT _chall_&param._UB;
        %if &S.=1 %then %do;
            %let _chall_&param._LB=%scan(&s1., 1, /);
            %let _chall_&param._INIT=%scan(&s1., 2, /);
            %let _chall_&param._UB=%scan(&s1., 3, /);
        %end;
        %else %if &S.=2 %then %do;
            %let _chall_&param._LB=%scan(&s2., 1, /);
            %let _chall_&param._INIT=%scan(&s2., 2, /);
            %let _chall_&param._UB=%scan(&s2., 3, /);
        %end;
        %else %if &S.=3 %then %do;
            %let _chall_&param._LB=%scan(&s3., 1, /);
            %let _chall_&param._INIT=%scan(&s3., 2, /);
            %let _chall_&param._UB=%scan(&s3., 3, /);
        %end;
        %else %do;
            %let _chall_&param._LB=%scan(&s4., 1, /);
            %let _chall_&param._INIT=%scan(&s4., 2, /);
            %let _chall_&param._UB=%scan(&s4., 3, /);
        %end;
    %mend _chall_base;

    %_chall_base(NTREES, 100/250/400, 200/400/600, 300/500/800, 300/600/1000);
    %_chall_base(LR, 0.05/0.07/0.10, 0.04/0.06/0.08, 0.03/0.05/0.07, 0.02/0.03/0.06);
    %_chall_base(MAXDEP, 2/3/4, 2/3/5, 2/3/5, 2/3/5);
    %_chall_base(LEAF, 25/60/100, 50/120/200, 100/150/200, 150/200/200);
    %_chall_base(SRATE, 0.70/0.85/1.00, 0.65/0.80/0.90, 0.60/0.75/0.85, 0.55/0.70/0.80);
    %_chall_base(VARSTR, 3/6/9, 5/10/15, 5/10/20, 10/15/25);
    %_chall_base(LASSO, 0/0.04/0.10, 0/0.05/0.12, 0/0.06/0.15, 0/0.07/0.15);
    %_chall_base(RIDGE, 0/0.6/2, 0/1/3, 0/1.5/4, 0/2/5);
    %_chall_base(NBIN, 32/64/128, 32/128/256, 64/128/256, 64/128/256);

    %if &P.=1 %then %do;
        %if &p_inputs. <= 10 %then %do;
            %let _chall_VARSTR_LB=%sysfunc(min(&p_inputs., 2));
            %let _chall_VARSTR_INIT=%sysfunc(min(&p_inputs., 3));
            %let _chall_VARSTR_UB=&p_inputs.;
        %end;
    %end;
    %else %if &P.=2 %then %do;
        %let _chall_MAXDEP_UB=%sysfunc(min(6, %sysevalf(&_chall_MAXDEP_UB. + 1)));
        %let _chall_VARSTR_LB=%sysfunc(max(1, %sysevalf(&_chall_VARSTR_LB. - 1)));
    %end;
    %else %do;
        %let _chall_NTREES_LB=%eval(&_chall_NTREES_LB. + 100);
        %let _chall_NTREES_INIT=%eval(&_chall_NTREES_INIT. + 100);
        %let _chall_NTREES_UB=%eval(&_chall_NTREES_UB. + 200);
        %let _chall_LR_LB=%sysfunc(max(0.01, %sysevalf(&_chall_LR_LB. - 0.01)));
        %let _chall_LR_INIT=%sysfunc(max(0.01, %sysevalf(&_chall_LR_INIT. - 0.01)));
        %let _chall_LR_UB=%sysfunc(max(0.01, %sysevalf(&_chall_LR_UB. - 0.01)));
        %let _chall_MAXDEP_UB=%sysfunc(min(6, %sysevalf(&_chall_MAXDEP_UB. + 1)));
        %let _chall_LEAF_LB=%eval(&_chall_LEAF_LB. + 25);
        %let _chall_LEAF_INIT=%eval(&_chall_LEAF_INIT. + 50);
        %let _chall_SRATE_LB=%sysevalf(&_chall_SRATE_LB. - 0.05);
        %let _chall_SRATE_INIT=%sysevalf(&_chall_SRATE_INIT. - 0.05);
        %let _chall_SRATE_UB=%sysevalf(&_chall_SRATE_UB. - 0.05);
        %let _chall_VARSTR_LB=%eval(&_chall_VARSTR_LB. + 5);
        %let _chall_VARSTR_INIT=%eval(&_chall_VARSTR_INIT. + 5);
        %let _chall_VARSTR_UB=%eval(&_chall_VARSTR_UB. + 5);
        %let _chall_LASSO_INIT=%sysfunc(min(0.20, %sysevalf(&_chall_LASSO_INIT. + 0.02)));
        %let _chall_LASSO_UB=%sysfunc(min(0.20, %sysevalf(&_chall_LASSO_UB. + 0.02)));
        %let _chall_RIDGE_INIT=%sysevalf(&_chall_RIDGE_INIT. + 0.5);
        %let _chall_RIDGE_UB=%sysevalf(&_chall_RIDGE_UB. + 0.5);
    %end;

    %if &_chall_NTREES_UB. > &ntrees_cap. %then %let _chall_NTREES_UB=&ntrees_cap.;
    %if &_chall_NTREES_INIT. > &_chall_NTREES_UB. %then %let _chall_NTREES_INIT=&_chall_NTREES_UB.;
    %if &_chall_NTREES_LB. > &_chall_NTREES_INIT. %then %let _chall_NTREES_LB=&_chall_NTREES_INIT.;

    %if &_chall_VARSTR_UB. > &p_inputs. %then %let _chall_VARSTR_UB=&p_inputs.;
    %if &_chall_VARSTR_INIT. > &p_inputs. %then %let _chall_VARSTR_INIT=&p_inputs.;
    %if &_chall_VARSTR_LB. > &p_inputs. %then %let _chall_VARSTR_LB=&p_inputs.;
    %if &_chall_VARSTR_LB. < 1 %then %let _chall_VARSTR_LB=1;
    %if &_chall_VARSTR_INIT. < &_chall_VARSTR_LB. %then %let _chall_VARSTR_INIT=&_chall_VARSTR_LB.;
    %if &_chall_VARSTR_UB. < &_chall_VARSTR_INIT. %then %let _chall_VARSTR_UB=&_chall_VARSTR_INIT.;

    %if &_chall_LEAF_UB. > &_chall_train_obs. %then %let _chall_LEAF_UB=&_chall_train_obs.;
    %if &_chall_LEAF_INIT. > &_chall_LEAF_UB. %then %let _chall_LEAF_INIT=&_chall_LEAF_UB.;
    %if &_chall_LEAF_LB. > &_chall_LEAF_INIT. %then %let _chall_LEAF_LB=&_chall_LEAF_INIT.;
    %if &_chall_LEAF_LB. < 1 %then %let _chall_LEAF_LB=1;

    %global _chall_ntrees_bounds _chall_lr_bounds _chall_maxdepth_bounds
        _chall_minleaf_bounds _chall_ssrate_bounds
        _chall_vars_to_try_bounds _chall_lasso_bounds _chall_ridge_bounds
        _chall_bins_bounds _chall_maxbranch_bounds;

    %let _chall_ntrees_bounds=LB=&_chall_NTREES_LB. UB=&_chall_NTREES_UB. INIT=&_chall_NTREES_INIT.;
    %let _chall_lr_bounds=LB=&_chall_LR_LB. UB=&_chall_LR_UB. INIT=&_chall_LR_INIT.;
    %let _chall_maxdepth_bounds=LB=&_chall_MAXDEP_LB. UB=&_chall_MAXDEP_UB. INIT=&_chall_MAXDEP_INIT.;
    %let _chall_minleaf_bounds=LB=&_chall_LEAF_LB. UB=&_chall_LEAF_UB. INIT=&_chall_LEAF_INIT.;
    %let _chall_ssrate_bounds=LB=&_chall_SRATE_LB. UB=&_chall_SRATE_UB. INIT=&_chall_SRATE_INIT.;
    %let _chall_vars_to_try_bounds=LB=&_chall_VARSTR_LB. UB=&_chall_VARSTR_UB. INIT=&_chall_VARSTR_INIT.;
    %let _chall_lasso_bounds=LB=&_chall_LASSO_LB. UB=&_chall_LASSO_UB. INIT=&_chall_LASSO_INIT.;
    %let _chall_ridge_bounds=LB=&_chall_RIDGE_LB. UB=&_chall_RIDGE_UB. INIT=&_chall_RIDGE_INIT.;
    %let _chall_bins_bounds=LB=&_chall_NBIN_LB. UB=&_chall_NBIN_UB. INIT=&_chall_NBIN_INIT.;
    %let _chall_maxbranch_bounds=2;
%mend _chall_gb_set_ranges;

%macro _chall_gb_tune(data=casuser._chall_train_part, num_input=,
    cat_input=, target_input=, gb_stagnation=0, nparallel=5,
    eval_out=work._chall_eval_history,
    best_out=work._chall_bestconfiguration);

    ods exclude all;
    proc gradboost data=&data.
        maxbranch=&_chall_maxbranch_bounds.
        seed=12345
        earlystop(metric=LOGLOSS stagnation=&gb_stagnation.);
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
                lasso(&_chall_lasso_bounds.)
                learningrate(&_chall_lr_bounds.)
                maxdepth(&_chall_maxdepth_bounds.)
                minleafsize(&_chall_minleaf_bounds.)
                ntrees(&_chall_ntrees_bounds.)
                numbin(&_chall_bins_bounds.)
                ridge(&_chall_ridge_bounds.)
                samplingrate(&_chall_ssrate_bounds.)
                vars_to_try(&_chall_vars_to_try_bounds.)
            );
        ods output BestConfiguration=&best_out.;
        ods output EvaluationHistory=&eval_out.;
    run;
    ods exclude none;

    data &eval_out.;
        set &eval_out.;
        if not missing(maxlevel) then maxlevel=maxlevel - 1;
    run;
%mend _chall_gb_tune;

%macro _chall_prepare_topk(eval_data=work._chall_eval_history, top_k=40,
    out=work._chall_topk_cfg);

    proc sort data=&eval_data. out=work._chall_hist_sorted;
        by descending GiniCoefficient;
    run;

    data &out.;
        length Algo_Name $32;
        set work._chall_hist_sorted(obs=&top_k.);
        cfg_id=_n_;
        model_rank=_n_;
        Algo_Name="Gradient Boosting";
    run;

    proc datasets library=work nolist nowarn;
        delete _chall_hist_sorted;
    quit;
%mend _chall_prepare_topk;
