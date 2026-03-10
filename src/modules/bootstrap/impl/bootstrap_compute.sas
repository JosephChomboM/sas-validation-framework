/* =========================================================================
bootstrap_compute.sas - Computo de bootstrapping de coeficientes

Macros:
%_boot_regresion       - PROC LOGISTIC estandar
%_boot_reg_ponderada   - PROC LOGISTIC ponderada (rebalanceo por evento)
%_boot_pesos           - Calculo de pesos por variable (|beta * mean_diff|)
%_boot_compute         - Orquestador: PROC SURVEYSELECT + PROC LOGISTIC,
                         construye tablaout, cubo_wide, report_final

Todas las operaciones usan Pattern B (work staging):
PROC SURVEYSELECT, PROC LOGISTIC, PROC SORT, PROC TRANSPOSE, PROC MEANS
no son CAS-compatibles.

Outputs (tablas work):
work._boot_tablaout     - Formato largo: Variable x Estimate x iteracion
work._boot_cubo_wide    - Formato wide: Variable x iter_1 ... iter_N
work._boot_report_final - Resumen: stats + percentiles + betas TRAIN/OOT
========================================================================= */

/* =====================================================================
%_boot_regresion - PROC LOGISTIC estandar (sin ponderacion)
Entrada: dataset work, target, lista_variables
Salida: out_betas (Variable, Estimate, StdErr, ProbChiSq)
===================================================================== */
%macro _boot_regresion(tablain, target, lista_variables, out_betas=);

    ods select none;
    proc logistic data=&tablain. namelen=45;
        model &target.(event="1") = &lista_variables.;
        ods output parameterestimates=&out_betas.;
    run;
    ods select all;

    data &out_betas.;
        set &out_betas.;
        where Variable ^= "Intercept";
    run;

%mend _boot_regresion;

/* =====================================================================
%_boot_reg_ponderada - PROC LOGISTIC con rebalanceo de clases
Rebalancea frecuencias minoritaria/mayoritaria via peso por evento.
Entrada: dataset work, variables, target, weight variable
Salida: out_betas (Variable, Estimate, StdErr, ProbChiSq)
===================================================================== */
%macro _boot_reg_ponderada(tablain, lista_variables, target, weight,
    out_betas=);

    %local rnd multiplier;
    %let rnd = %sysfunc(int(%sysfunc(ranuni(0))*100000));

    ods select none;
    proc freq data=&tablain.;
        table &target. / missing out=work._btrp_frq_&rnd.;
        weight &weight.;
    run;

    data _null_;
        set work._btrp_frq_&rnd.;
        where &target. = 1;
        multiplier = (100 - percent) / percent;
        call symput("multiplier", left(trim(put(multiplier, 20.10))));
    run;

    data work._btrp_wgt_&rnd.;
        set &tablain.;
        if &target. = 1 then sample_w = &weight. * &multiplier.;
        else sample_w = &weight.;
    run;

    proc logistic data=work._btrp_wgt_&rnd. namelen=45;
        model &target.(event="1") = &lista_variables.;
        weight sample_w;
        ods output parameterestimates=&out_betas.;
    run;
    ods select all;

    data &out_betas.;
        set &out_betas.;
        where Variable ^= "Intercept";
    run;

    proc datasets library=work nolist nowarn;
        delete _btrp_frq_&rnd. _btrp_wgt_&rnd.;
    quit;

%mend _boot_reg_ponderada;

/* =====================================================================
%_boot_pesos - Calculo de pesos por variable
peso = |beta * (mean_def1 - mean_def0)| / sum(todos)
===================================================================== */
%macro _boot_pesos(data, betas, target, lista_variables, out_pesos=);

    %local rnd _bt_sum_pond;
    %let rnd = %sysfunc(int(%sysfunc(ranuni(0))*100000));
    %let _bt_sum_pond = 0;

    proc sort data=&data. out=work._btp_pw_&rnd._1;
        by &target.;
    run;

    ods select none;
    proc means data=work._btp_pw_&rnd._1 stackods;
        var &lista_variables.;
        class &target.;
        ods output summary=work._btp_pw_&rnd._2;
    run;
    ods select all;

    proc sort data=work._btp_pw_&rnd._2;
        by Variable;
    run;

    proc transpose data=work._btp_pw_&rnd._2 prefix=def
        out=work._btp_pw_&rnd._3(drop=_NAME_);
        by Variable;
        id &target.;
        var Mean;
    run;

    proc sql;
        create table work._btp_pw_&rnd._4 as
        select a.Variable, a.Estimate, abs(b.def1 - b.def0) as diff
        from &betas. a
        left join work._btp_pw_&rnd._3 b
            on upcase(a.Variable) = upcase(b.Variable)
        where a.Variable not in ("Intercept");
    quit;

    data &out_pesos.;
        set work._btp_pw_&rnd._4;
        pond = abs(Estimate * diff);
    run;

    proc sql noprint;
        select sum(pond) into :_bt_sum_pond trimmed from &out_pesos.;
    quit;

    data &out_pesos.;
        set &out_pesos.;
        %if %sysevalf(&_bt_sum_pond. > 0) %then %do;
            peso = pond / &_bt_sum_pond.;
        %end;
        %else %do;
            peso = 0;
        %end;
    run;

    proc datasets library=work nolist nowarn;
        delete _btp_pw_&rnd.:;
    quit;

%mend _boot_pesos;

/* =====================================================================
%_boot_compute - Orquestador principal de bootstrapping

1) Sort TRAIN por target
2) PROC SURVEYSELECT (bootstrap estratificado)
3) PROC LOGISTIC por replicado (o loop para ponderada)
4) Modelos finales TRAIN + OOT + pesos
5) Cubo wide (transposicion)
6) Estadisticas de estabilidad de signo
7) Percentiles
8) Report final

Outputs en work (sin sufijo rnd - outputs estables):
_boot_tablaout     - largo: Variable x Estimate x iteracion
_boot_cubo_wide    - wide: Variable x iter_1 ... iter_N
_boot_report_final - resumen con stats + percentiles + betas TRAIN/OOT
===================================================================== */
%macro _boot_compute(train_data=, oot_data=, lista_variables=, target=,
    nrounds=100, samprate=1, seed=12345, ponderada=0);

    %local rnd i;
    %let rnd = %sysfunc(int(%sysfunc(ranuni(0))*100000));

    /* ---- 1. Sort TRAIN por target ------------------------------------- */
    proc sort data=&train_data. out=work._bt_trn_&rnd.;
        by &target.;
    run;

    /* ---- 2-3. Bootstrap + Logistic ------------------------------------ */
    %if &ponderada. = 0 %then %do;

        /* --- No ponderada: reps + BY Replicate (rapido, sin loop) ------ */
        ods select none;
        proc surveyselect data=work._bt_trn_&rnd.
            out=work._bt_ss_&rnd.
            seed=&seed.
            method=urs
            samprate=&samprate.
            nmin=1
            outhits
            reps=&nrounds.;
            strata &target.;
        run;

        proc sort data=work._bt_ss_&rnd.;
            by Replicate;
        run;

        proc logistic data=work._bt_ss_&rnd. namelen=45;
            by Replicate;
            model &target.(event="1") = &lista_variables.;
            ods output parameterestimates=work._bt_betas_&rnd.;
        run;
        ods select all;

        data work._boot_tablaout;
            set work._bt_betas_&rnd.;
            where Variable ^= "Intercept";
            iteracion = Replicate;
            keep Variable Estimate StdErr ProbChiSq iteracion;
        run;

    %end;
    %else %do;

        /* --- Ponderada: loop con rebalanceo por evento ----------------- */
        ods select none;
        %do i = 1 %to &nrounds.;

            proc surveyselect data=work._bt_trn_&rnd. noprint
                out=work._bt_iter_&rnd.
                seed=&i.
                method=urs
                samprate=&samprate.
                outhits;
                strata &target.;
            run;

            data work._bt_iter_&rnd.;
                set work._bt_iter_&rnd.;
                numhits = 1;
            run;

            %_boot_reg_ponderada(work._bt_iter_&rnd., &lista_variables.,
                &target., numhits, out_betas=work._bt_ibetas_&rnd.);

            %if &i. = 1 %then %do;
                proc sql;
                    create table work._boot_tablaout as
                    select Variable, Estimate, StdErr, ProbChiSq,
                        &i. as iteracion
                    from work._bt_ibetas_&rnd.
                    where Variable ^= "Intercept";
                quit;
            %end;
            %else %do;
                proc sql;
                    insert into work._boot_tablaout
                    select Variable, Estimate, StdErr, ProbChiSq,
                        &i. as iteracion
                    from work._bt_ibetas_&rnd.
                    where Variable ^= "Intercept";
                quit;
            %end;

        %end;
        ods select all;

    %end;

    /* ---- 4. Modelos finales TRAIN + OOT + pesos ----------------------- */
    %if &ponderada. = 0 %then %do;

        %_boot_regresion(&train_data., &target., &lista_variables.,
            out_betas=work._bt_btrn_&rnd.);
        %_boot_pesos(&train_data., work._bt_btrn_&rnd., &target.,
            &lista_variables., out_pesos=work._bt_ptrn_&rnd.);

        %_boot_regresion(&oot_data., &target., &lista_variables.,
            out_betas=work._bt_boot_&rnd.);
        %_boot_pesos(&oot_data., work._bt_boot_&rnd., &target.,
            &lista_variables., out_pesos=work._bt_poot_&rnd.);

    %end;
    %else %do;

        data work._bt_trn_w_&rnd.;
            set &train_data.;
            numhits = 1;
        run;

        data work._bt_oot_w_&rnd.;
            set &oot_data.;
            numhits = 1;
        run;

        %_boot_reg_ponderada(work._bt_trn_w_&rnd., &lista_variables.,
            &target., numhits, out_betas=work._bt_btrn_&rnd.);
        %_boot_pesos(work._bt_trn_w_&rnd., work._bt_btrn_&rnd., &target.,
            &lista_variables., out_pesos=work._bt_ptrn_&rnd.);

        %_boot_reg_ponderada(work._bt_oot_w_&rnd., &lista_variables.,
            &target., numhits, out_betas=work._bt_boot_&rnd.);
        %_boot_pesos(work._bt_oot_w_&rnd., work._bt_boot_&rnd., &target.,
            &lista_variables., out_pesos=work._bt_poot_&rnd.);

    %end;

    /* Join betas + pesos para TRAIN */
    proc sql;
        create table work._bt_jtrn_&rnd. as
        select a.Variable, a.Estimate, a.StdErr, a.ProbChiSq,
            b.peso format=8.4
        from work._bt_btrn_&rnd. a
        left join work._bt_ptrn_&rnd. b
            on upcase(a.Variable) = upcase(b.Variable);
    quit;

    /* Join betas + pesos para OOT */
    proc sql;
        create table work._bt_joot_&rnd. as
        select a.Variable, a.Estimate, a.StdErr, a.ProbChiSq,
            b.peso format=8.4
        from work._bt_boot_&rnd. a
        left join work._bt_poot_&rnd. b
            on upcase(a.Variable) = upcase(b.Variable);
    quit;

    /* ---- 5. Cubo wide (transposicion) --------------------------------- */
    proc sort data=work._boot_tablaout;
        by Variable iteracion;
    run;

    proc transpose data=work._boot_tablaout
        out=work._boot_cubo_wide(drop=_NAME_) prefix=iter_;
        by Variable;
        id iteracion;
        var Estimate;
    run;

    /* ---- 6. Estadisticas de estabilidad de signo ---------------------- */
    proc sql;
        create table work._bt_stats_&rnd. as
        select
            Variable,
            count(*) as n_iter,
            mean(Estimate) as beta_mean format=12.6,
            std(Estimate) as beta_std format=12.6,
            min(Estimate) as beta_min format=12.6,
            max(Estimate) as beta_max format=12.6,
            calculated beta_mean - 1.96 * calculated beta_std
                as ci_lower_95 format=12.6,
            calculated beta_mean + 1.96 * calculated beta_std
                as ci_upper_95 format=12.6,
            sum(case when Estimate > 0 then 1 else 0 end) as n_positivos,
            sum(case when Estimate < 0 then 1 else 0 end) as n_negativos,
            sum(case when Estimate = 0 then 1 else 0 end) as n_ceros,
            max(
                sum(case when Estimate > 0 then 1 else 0 end),
                sum(case when Estimate < 0 then 1 else 0 end)
            ) / count(*) as pct_signo_consistente format=percent8.1,
            case
                when sum(case when Estimate > 0 then 1 else 0 end) > 0
                 and sum(case when Estimate < 0 then 1 else 0 end) > 0
                then 'INESTABLE'
                else 'ESTABLE'
            end as flag_signo length=10
        from work._boot_tablaout
        group by Variable;
    quit;

    /* Join con betas TRAIN/OOT */
    proc sql;
        create table work._bt_rpt_&rnd. as
        select
            a.*,
            b.Estimate as beta_dev format=12.6,
            b.StdErr as stderr_dev format=12.6,
            b.ProbChiSq as pval_dev format=8.4,
            b.peso as peso_dev format=8.4,
            c.Estimate as beta_oot format=12.6,
            c.StdErr as stderr_oot format=12.6,
            c.ProbChiSq as pval_oot format=8.4,
            c.peso as peso_oot format=8.4,
            case
                when b.Estimate > 0 and a.n_negativos > 0
                then 'TRAIN+ pero Bootstrap tiene negativos'
                when b.Estimate < 0 and a.n_positivos > 0
                then 'TRAIN- pero Bootstrap tiene positivos'
                else 'Consistente'
            end as alerta_signo length=50
        from work._bt_stats_&rnd. a
        left join work._bt_jtrn_&rnd. b
            on upcase(a.Variable) = upcase(b.Variable)
        left join work._bt_joot_&rnd. c
            on upcase(a.Variable) = upcase(c.Variable);
    quit;

    /* ---- 7. Percentiles ----------------------------------------------- */
    proc sort data=work._boot_tablaout;
        by Variable;
    run;

    proc means data=work._boot_tablaout noprint;
        by Variable;
        var Estimate;
        output out=work._bt_pctl_&rnd.(drop=_TYPE_ _FREQ_)
            p1=p1 p5=p5 p10=p10 p25=p25 p50=p50
            p75=p75 p90=p90 p95=p95 p99=p99;
    run;

    /* ---- 8. Report final: merge stats + percentiles ------------------- */
    proc sql;
        create table work._boot_report_final as
        select a.*,
            b.p1, b.p5, b.p10, b.p25, b.p50,
            b.p75, b.p90, b.p95, b.p99
        from work._bt_rpt_&rnd. a
        left join work._bt_pctl_&rnd. b
            on a.Variable = b.Variable
        order by a.peso_dev desc;
    quit;

    /* ---- Cleanup temporales ------------------------------------------- */
    proc datasets library=work nolist nowarn;
        delete _bt_trn_&rnd. _bt_ss_&rnd. _bt_betas_&rnd.
            _bt_iter_&rnd. _bt_ibetas_&rnd.
            _bt_trn_w_&rnd. _bt_oot_w_&rnd.
            _bt_btrn_&rnd. _bt_boot_&rnd.
            _bt_ptrn_&rnd. _bt_poot_&rnd.
            _bt_jtrn_&rnd. _bt_joot_&rnd.
            _bt_stats_&rnd. _bt_rpt_&rnd. _bt_pctl_&rnd.;
    quit;

    %put NOTE: [bootstrap_compute] Tablas de salida:;
    %put NOTE: [bootstrap_compute] work._boot_tablaout (largo);
    %put NOTE: [bootstrap_compute] work._boot_cubo_wide (wide);
    %put NOTE: [bootstrap_compute] work._boot_report_final (resumen);

%mend _boot_compute;
