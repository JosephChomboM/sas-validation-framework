/* =========================================================================
replica_compute.sas - Computo del modulo Replica

Replica conserva la logica del legacy:
- PROC LOGISTIC (estandar o ponderada)
- Pesos por variable = |beta * diff_medias|
- Supuestos: VIF, normalidad, Levene, Durbin-Watson
- Control opcional: contraste entre y_est y PD/XB/TARGET

Todas las operaciones usan Pattern B (work staging) porque PROC LOGISTIC,
PROC REG, PROC AUTOREG, PROC GLM, PROC TRANSPOSE y PROC MEANS no son
CAS-first en este flujo.
========================================================================= */

%macro _rep_regresion(tablain=, target=, lista_variables=, out_prefix=,
    keep_intercept=1);

    ods select none;
    proc logistic data=&tablain. namelen=45;
        model &target.(event="1")=&lista_variables.;
        output out=work.&out_prefix._out pred=y_est;
        ods output NObs=work.&out_prefix._nobs;
        ods output ResponseProfile=work.&out_prefix._responseprofile;
        ods output FitStatistics=work.&out_prefix._fitstatistics;
        ods output ParameterEstimates=work.&out_prefix._betas_raw;
        ods output Association=work.&out_prefix._stats;
    run;
    ods select all;

    data work.&out_prefix._betas;
        set work.&out_prefix._betas_raw;
        %if &keep_intercept.=0 %then %do;
            where Variable ^= "Intercept";
        %end;
    run;

%mend _rep_regresion;

%macro _rep_reg_ponderada(tablain=, lista_variables=, target=, weight=,
    out_prefix=, keep_intercept=1);

    %local rnd multiplier;
    %let rnd=%sysfunc(int(%sysfunc(ranuni(0))*100000));

    ods select none;
    proc freq data=&tablain.;
        table &target. / missing out=work._rep_frq_&rnd.;
        weight &weight.;
    run;
    ods select all;

    data _null_;
        set work._rep_frq_&rnd.;
        where &target.=1;
        if percent > 0 then multiplier=(100 - percent) / percent;
        else multiplier=1;
        call symputx('multiplier', multiplier);
    run;

    %if %length(%superq(multiplier))=0 %then %let multiplier=1;

    data work._rep_wgt_&rnd.;
        set &tablain.;
        if &target.=1 then sample_w=&weight. * &multiplier.;
        else sample_w=&weight.;
    run;

    ods select none;
    proc logistic data=work._rep_wgt_&rnd. namelen=45;
        model &target.(event="1")=&lista_variables.;
        weight sample_w;
        output out=work.&out_prefix._out pred=y_est;
        ods output NObs=work.&out_prefix._nobs;
        ods output ResponseProfile=work.&out_prefix._responseprofile;
        ods output FitStatistics=work.&out_prefix._fitstatistics;
        ods output ParameterEstimates=work.&out_prefix._betas_raw;
        ods output Association=work.&out_prefix._stats;
    run;
    ods select all;

    data work.&out_prefix._betas;
        set work.&out_prefix._betas_raw;
        %if &keep_intercept.=0 %then %do;
            where Variable ^= "Intercept";
        %end;
    run;

    proc datasets library=work nolist nowarn;
        delete _rep_frq_&rnd. _rep_wgt_&rnd.;
    quit;

%mend _rep_reg_ponderada;

%macro _rep_pesos(data=, betas=, target=, lista_variables=, out_table=);

    %local rnd _sum_pond;
    %let rnd=%sysfunc(int(%sysfunc(ranuni(0))*100000));
    %let _sum_pond=0;

    proc sort data=&data. out=work._rep_pw_&rnd._1;
        by &target.;
    run;

    ods select none;
    proc means data=work._rep_pw_&rnd._1 stackods;
        var &lista_variables.;
        class &target.;
        ods output Summary=work._rep_pw_&rnd._2;
    run;
    ods select all;

    proc sort data=work._rep_pw_&rnd._2;
        by Variable;
    run;

    proc transpose data=work._rep_pw_&rnd._2
        out=work._rep_pw_&rnd._3(drop=_NAME_) prefix=def;
        by Variable;
        id &target.;
        var Mean;
    run;

    proc sql;
        create table work._rep_pw_&rnd._4 as
        select a.Variable, a.Estimate,
            abs(coalesce(b.def1, 0) - coalesce(b.def0, 0)) as diff
        from &betas. a
        left join work._rep_pw_&rnd._3 b
            on upcase(a.Variable)=upcase(b.Variable)
        where a.Variable not in ("Intercept");
    quit;

    data &out_table.;
        set work._rep_pw_&rnd._4;
        pond=abs(Estimate * diff);
    run;

    proc sql noprint;
        select sum(pond) into :_sum_pond trimmed
        from &out_table.;
    quit;

    %if %length(%superq(_sum_pond))=0 %then %let _sum_pond=0;

    data &out_table.;
        set &out_table.;
        if &_sum_pond. > 0 then peso=pond / &_sum_pond.;
        else peso=0;
    run;

    proc datasets library=work nolist nowarn;
        delete _rep_pw_&rnd.:;
    quit;

%mend _rep_pesos;

%macro _rep_control_stats(data=, control_var=, out_table=);

    %local rnd;
    %let rnd=%sysfunc(int(%sysfunc(ranuni(0))*100000));

    proc corr data=&data. noprint outp=work._rep_ctrlcorr_&rnd.;
        var y_est &control_var.;
    run;

    data work._rep_ctrlval_&rnd.;
        set work._rep_ctrlcorr_&rnd.;
        where _TYPE_='CORR' and upcase(_NAME_)='Y_EST';
        length control_var $64;
        control_var="&control_var.";
        corr_y_est=input(vvaluex("&control_var."), best32.);
        keep control_var corr_y_est;
    run;

    proc sql;
        create table work._rep_ctrlsum_&rnd. as
        select "&control_var." as control_var length=64,
            count(*) as n_obs,
            mean(y_est) as prom_y_est format=12.6,
            mean(&control_var.) as prom_control format=12.6,
            mean(abs(y_est - &control_var.)) as mae format=12.6,
            sqrt(mean((y_est - &control_var.) * (y_est - &control_var.)))
                as rmse format=12.6
        from &data.
        where not missing(y_est) and not missing(&control_var.);
    quit;

    proc sql;
        create table &out_table. as
        select a.*, b.corr_y_est format=12.6
        from work._rep_ctrlsum_&rnd. a
        left join work._rep_ctrlval_&rnd. b
            on upcase(a.control_var)=upcase(b.control_var);
    quit;

    proc datasets library=work nolist nowarn;
        delete _rep_ctrlcorr_&rnd. _rep_ctrlval_&rnd. _rep_ctrlsum_&rnd.;
    quit;

%mend _rep_control_stats;

%macro _rep_calcular_cortes(data=, var=, groups=10, out_cuts=, out_ranked=);

    %local rnd;
    %let rnd=%sysfunc(int(%sysfunc(ranuni(0))*100000));

    data work._rep_cutbase_&rnd.;
        set &data.(keep=residuals &var.);
        where not missing(&var.);
    run;

    proc rank data=work._rep_cutbase_&rnd. out=work._rep_rank_&rnd.
        groups=&groups.;
        var &var.;
        ranks rango;
    run;

    proc sql;
        create table work._rep_cutsum_&rnd. as
        select rango,
            min(&var.) as inicio format=12.6,
            max(&var.) as fin format=12.6
        from work._rep_rank_&rnd.
        group by rango
        order by rango;
    quit;

    data &out_cuts.;
        set work._rep_cutsum_&rnd.;
        length variable $64 etiqueta $200;
        variable="&var.";
        rango=rango + 1;
        etiqueta=catx(' ', cats(put(rango, z2.), '.'), '[',
            strip(put(inicio, 12.4)), ';', strip(put(fin, 12.4)), ']');
    run;

    data &out_ranked.;
        set work._rep_rank_&rnd.;
        rango=rango + 1;
    run;

    proc datasets library=work nolist nowarn;
        delete _rep_cutbase_&rnd. _rep_rank_&rnd. _rep_cutsum_&rnd.;
    quit;

%mend _rep_calcular_cortes;

%macro _rep_supuestos_regresion(dataset=, alias=, target=, vars_num=,
    time_var=, groups=10);

    proc format;
        value vif_fmt
            0 -< 5 = 'lightgreen'
            5 -< 10 = 'yellow'
            10 - high = 'lightred';
        value levene_fmt
            0 -< 0.05 = 'lightred'
            0.05 - high = 'lightgreen';
        value dw_fmt
            0 -< 1.5 = 'lightred'
            1.5 -< 2.5 = 'lightgreen'
            2.5 - high = 'lightred';
    run;

    ods select none;
    proc reg data=&dataset outest=work.&alias._model_params;
        model &target.=&vars_num. / vif;
        output out=work.&alias._residuals
            predicted=predicted residual=residuals student=studentized;
        ods output ParameterEstimates=work.&alias._vif;
    run;
    quit;
    ods select all;

    ods select none;
    proc univariate data=work.&alias._residuals normal;
        var residuals;
        ods output TestsForNormality=work.&alias._normality;
    run;
    ods select all;

    %_rep_calcular_cortes(data=work.&alias._residuals, var=predicted,
        groups=&groups., out_cuts=work.&alias._cuts,
        out_ranked=work.&alias._ranked);

    ods select none;
    proc glm data=work.&alias._ranked;
        class rango;
        model residuals=rango;
        means rango / hovtest=levene(type=abs);
        ods output HOVFTest=work.&alias._levene;
    run;
    quit;
    ods select all;

    %if %length(%superq(time_var)) > 0 %then %do;
        proc sort data=work.&alias._residuals
            out=work.&alias._residuals_sorted;
            by &time_var.;
        run;

        ods select none;
        proc autoreg data=work.&alias._residuals_sorted;
            model &target.=&vars_num. / dw=2 dwprob;
            output out=work.&alias._dw_residuals p=p r=r;
            ods output DWTest=work.&alias._dw;
        run;
        quit;
        ods select all;
    %end;

%mend _rep_supuestos_regresion;

%macro _replica_compute(data=, alias=, target=, lista_var=, ponderada=1,
    hits=1, groups=10, time_var=, control_var=);

    data work.&alias._base;
        set &data.;
        numhits=&hits.;
    run;

    %if &ponderada.=1 %then %do;
        %_rep_reg_ponderada(tablain=work.&alias._base,
            lista_variables=&lista_var., target=&target., weight=numhits,
            out_prefix=&alias., keep_intercept=1);
    %end;
    %else %do;
        %_rep_regresion(tablain=work.&alias._base, target=&target.,
            lista_variables=&lista_var., out_prefix=&alias.,
            keep_intercept=1);
    %end;

    %_rep_pesos(data=work.&alias._base, betas=work.&alias._betas,
        target=&target., lista_variables=&lista_var.,
        out_table=work.&alias._pesos);

    proc sql;
        create table work.&alias._betas_report as
        select a.Variable, a.Estimate, a.StdErr, a.ProbChiSq,
            b.peso format=8.4
        from work.&alias._betas a
        left join work.&alias._pesos b
            on upcase(a.Variable)=upcase(b.Variable)
        order by calculated peso desc, a.Variable;
    quit;

    %if %length(%superq(control_var)) > 0 %then %do;
        %_rep_control_stats(data=work.&alias._out, control_var=&control_var.,
            out_table=work.&alias._control);
    %end;

    %_rep_supuestos_regresion(dataset=work.&alias._base, alias=&alias.,
        target=&target., vars_num=&lista_var., time_var=&time_var.,
        groups=&groups.);

%mend _replica_compute;
