/* =========================================================================
challenge_champion.sas - Resumen de champion para METOD9
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
        %sysevalf(&penalty_lambda. *
        %sysevalf(&_chall_bmk_g_train. - &_chall_bmk_g_oot.)));

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

    %_chall_gini_monthly(data=&full_data., target=&target.,
        score_var=&score_var., byvar=&byvar., model_rank=0,
        model_label=Benchmark Base, algo_name=BASELINE, source=BENCHMARK,
        out=&out_monthly.);
%mend _chall_build_benchmark;

%macro _chall_build_global_summary(scored_train=, scored_oot=, target=,
    score_var=PD_FINAL, benchmark=work._chall_benchmark_global,
    penalty_lambda=0.5, scope=, champion_mode=SINGLE_MODEL,
    n_selected_models=1, out=work._chall_global_summary);

    %local _g_train _g_oot _g_penal _bmk_train _bmk_oot _bmk_penal;

    %_chall_gini(data=&scored_train., target=&target., score_var=&score_var.,
        outmac=_chall_glb_g_train);
    %_chall_gini(data=&scored_oot., target=&target., score_var=&score_var.,
        outmac=_chall_glb_g_oot);
    %let _g_train=&_chall_glb_g_train.;
    %let _g_oot=&_chall_glb_g_oot.;
    %let _g_penal=%sysevalf(&_g_oot. -
        %sysevalf(&penalty_lambda. * %sysevalf(&_g_train. - &_g_oot.)));

    proc sql noprint;
        select Gini into :_bmk_train trimmed
        from &benchmark.
        where upcase(Dataset)='TRAIN';
        select Gini into :_bmk_oot trimmed
        from &benchmark.
        where upcase(Dataset)='OOT';
        select Gini_Penalizado into :_bmk_penal trimmed
        from &benchmark.
        where upcase(Dataset)='OOT';
    quit;

    data &out.;
        length Scope $16 Champion_Mode $32;
        Scope="&scope.";
        Champion_Mode="&champion_mode.";
        N_Selected_Models=&n_selected_models.;
        Gini_Train=&_g_train.;
        Gini_OOT=&_g_oot.;
        Gini_Penalizado=&_g_penal.;
        Benchmark_Gini_Train=&_bmk_train.;
        Benchmark_Gini_OOT=&_bmk_oot.;
        Benchmark_Gini_Penalizado=&_bmk_penal.;
        Improvement_Train=Gini_Train - Benchmark_Gini_Train;
        Improvement_OOT=Gini_OOT - Benchmark_Gini_OOT;
        Improvement_Penalizado=Gini_Penalizado - Benchmark_Gini_Penalizado;
        format Gini_: Benchmark_: Improvement_: 8.4;
    run;
%mend _chall_build_global_summary;

%macro _chall_build_champion_summary(registry=work._chall_registry,
    benchmark=work._chall_benchmark_global,
    out=work._chall_champion_summary);

    %local _bmk_train _bmk_oot _bmk_penal;

    proc sql noprint;
        select Gini into :_bmk_train trimmed
        from &benchmark.
        where upcase(Dataset)='TRAIN';
        select Gini into :_bmk_oot trimmed
        from &benchmark.
        where upcase(Dataset)='OOT';
        select Gini_Penalizado into :_bmk_penal trimmed
        from &benchmark.
        where upcase(Dataset)='OOT';
    quit;

    proc sort data=&registry.(where=(Is_Champion=1))
        out=work._chall_champion_best;
        by descending Gini_Penalizado descending Gini_OOT descending Gini_Train;
    run;

    data &out.;
        set work._chall_champion_best(obs=1);
        Benchmark_Gini_Train=&_bmk_train.;
        Benchmark_Gini_OOT=&_bmk_oot.;
        Benchmark_Gini_Penalizado=&_bmk_penal.;
        Improvement_Train=Gini_Train - Benchmark_Gini_Train;
        Improvement_OOT=Gini_OOT - Benchmark_Gini_OOT;
        Improvement_Penalizado=Gini_Penalizado - Benchmark_Gini_Penalizado;
        format Benchmark_: Improvement_: 8.4;
    run;

    proc datasets library=work nolist nowarn;
        delete _chall_champion_best;
    quit;
%mend _chall_build_champion_summary;
