/* =========================================================================
challenge_champion.sas - Resumen de champion para METOD9
========================================================================= */

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
