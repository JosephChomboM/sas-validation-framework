/* =========================================================================
psi_compute.sas - Core de computo del PSI

Contiene dos macros:

1) %_psi_calc - PSI para UNA variable (core unitario)
   Input:  dos datasets (dev, oot) y nombre de variable
   Output: macro variable &psi_valor con el valor PSI

2) %_psi_compute - Orquestador: itera variables x periodos
   Input:  tablas CAS promovidas (via caslib.table), listas de vars,
           variable temporal (byvar), parametros de discretizacion
   Output: tres tablas en casuser (CAS):
           - casuser._psi_cubo       Variable x Periodo x PSI x Tipo
           - casuser._psi_cubo_wide  pivot Variable x meses + PSI_Total
           - casuser._psi_resumen    estadisticas + semaforo + alertas tendencia

Patron CAS: las tablas input se copian de CAS a work para procesamiento
local (PROC RANK, INSERT INTO, PROC SORT, PROC TRANSPOSE no soportan
CAS como destino directo). Los 3 resultados finales se copian a casuser
al terminar. work se limpia al final.
========================================================================= */

/* =====================================================================
   %_psi_calc - PSI para una variable
   Metodo: PROC RANK (continuas) o valores directos (categoricas)
   Suavizado Laplace: (n + 0.5) / (total + 0.5 * n_buckets)

   NOTA: dev= y oot= deben ser tablas en work (sin prefijo de libreria).
   Todas las tablas temporales se crean en work.
   ===================================================================== */
%macro _psi_calc(dev=, oot=, oot_where=, var=, n_buckets=10, flg_continue=1);

    %local rnd n_cortes i;
    %let rnd=%sysfunc(int(%sysfunc(ranuni(0))*100000));

    %global psi_valor;
    %let psi_valor=0;

    /*----- Variables continuas: crear buckets con PROC RANK -----*/
    %if &flg_continue.=1 %then %do;

        proc rank data=&dev.(keep=&var.) out=_rank_dev_&rnd. groups=&n_buckets.;
            var &var.;
            ranks bucket;
        run;

        proc sql noprint;
            create table _cortes_&rnd. as select bucket, max(&var.) as corte
                from _rank_dev_&rnd. where bucket is not missing group by bucket
                order by bucket;

            select count(*) into :n_cortes trimmed from _cortes_&rnd.;
        quit;

        %if &n_cortes. > 0 %then %do;
            proc sql noprint;
                select corte into :corte1 - :corte&n_cortes. from _cortes_&rnd.;
            quit;

            /* Asignar buckets a DEV usando los cortes */
            data _dev_bucket_&rnd.;
                set &dev.(keep=&var.);
                if missing(&var.) then bucket=0;
                %do i=1 %to &n_cortes.;
                    %if &i.=1 %then %do;
                        else if &var. <= &&corte&i. then bucket=&i.;
                    %end;
                    %else %if &i.=&n_cortes. %then %do;
                        else bucket=&i.;
                    %end;
                    %else %do;
                        else if &var. <= &&corte&i. then bucket=&i.;
                    %end;
                %end;
            run;

            /* Asignar buckets a OOT usando los mismos cortes */
            data _oot_bucket_&rnd.;
                %if %length(%superq(oot_where)) > 0 %then %do;
                    set &oot.(where=(&oot_where.));
                %end;
                %else %do;
                    set &oot.(keep=&var.);
                %end;
                if missing(&var.) then bucket=0;
                %do i=1 %to &n_cortes.;
                    %if &i.=1 %then %do;
                        else if &var. <= &&corte&i. then bucket=&i.;
                    %end;
                    %else %if &i.=&n_cortes. %then %do;
                        else bucket=&i.;
                    %end;
                    %else %do;
                        else if &var. <= &&corte&i. then bucket=&i.;
                    %end;
                %end;
            run;
        %end;
        %else %do;
            data _dev_bucket_&rnd.;
                set &dev.(keep=&var.);
                bucket=1;
            run;

            data _oot_bucket_&rnd.;
                %if %length(%superq(oot_where)) > 0 %then %do;
                    set &oot.(where=(&oot_where.));
                %end;
                %else %do;
                    set &oot.(keep=&var.);
                %end;
                bucket=1;
            run;
        %end;

    %end;
    /*----- Variables categoricas: usar valores como bucket -----*/
    %else %do;

        data _dev_bucket_&rnd.;
            set &dev.(keep=&var.);
            bucket=&var.;
        run;

        data _oot_bucket_&rnd.;
            %if %length(%superq(oot_where)) > 0 %then %do;
                set &oot.(where=(&oot_where.));
            %end;
            %else %do;
                set &oot.(keep=&var.);
            %end;
            bucket=&var.;
        run;

    %end;

    /*----- Calcular frecuencias y PSI con SQL -----*/
    proc sql noprint;
        create table _freq_dev_&rnd. as select bucket, count(*) as n_dev from
            _dev_bucket_&rnd. group by bucket;

        create table _freq_oot_&rnd. as select bucket, count(*) as n_oot from
            _oot_bucket_&rnd. group by bucket;

        select coalesce(sum(n_dev), 0) into :tot_dev trimmed from
            _freq_dev_&rnd.;
        select coalesce(sum(n_oot), 0) into :tot_oot trimmed from
            _freq_oot_&rnd.;
    quit;

    %if &tot_dev. > 0 and &tot_oot. > 0 %then %do;
        proc sql noprint;
            create table _psi_calc_&rnd. as select coalesce(a.bucket, b.bucket)
                as bucket, coalesce(a.n_dev, 0) as n_dev, coalesce(b.n_oot, 0)
                as n_oot, (coalesce(a.n_dev, 0) + 0.5) / (&tot_dev. + 0.5 *
                &n_buckets.) as pct_dev, (coalesce(b.n_oot, 0) + 0.5) /
                (&tot_oot. + 0.5 * &n_buckets.) as pct_oot from _freq_dev_&rnd.
                a full join _freq_oot_&rnd. b on a.bucket=b.bucket;

            select sum((pct_oot - pct_dev) * log(pct_oot / pct_dev)) into
                :psi_valor trimmed from _psi_calc_&rnd.;
        quit;
    %end;

    %if %length(&psi_valor.)=0 or &psi_valor.=. %then %let psi_valor=0;

    /* Cleanup temporales (work) */
    proc datasets lib=work nolist nowarn;
        delete _rank_dev_&rnd. _cortes_&rnd. _dev_bucket_&rnd. _oot_bucket_&rnd.
            _freq_dev_&rnd. _freq_oot_&rnd. _psi_calc_&rnd.;
    quit;

%mend _psi_calc;

/* =====================================================================
   %_psi_compute - Orquestador: variables x periodos -> cubo + wide + resumen

   Patron CAS:
     1) Copia tablas CAS (input_caslib) a work para procesamiento local
     2) Toda la iteracion (INSERT INTO, PROC SORT, PROC TRANSPOSE) corre
        en work - estas operaciones no estan soportadas en CAS
     3) Los 3 resultados finales se copian a casuser al terminar
     4) work se limpia al final

   Output final en casuser (CAS):
     casuser._psi_cubo        detalle Variable x Periodo x PSI x Tipo
     casuser._psi_cubo_wide   pivot Variable x meses + PSI_Total
     casuser._psi_resumen     estadisticas + semaforo + alertas tendencia

   Parametros:
     input_caslib  = CASLIB de las tablas promovidas
     train_table   = nombre tabla TRAIN promovida
     oot_table     = nombre tabla OOT promovida
     vars_num      = lista de variables numericas (separadas por espacio)
     vars_cat      = lista de variables categoricas (separadas por espacio)
     byvar         = variable temporal para breakdown mensual
     n_buckets     = numero de bins para PROC RANK (default 10)
     mensual       = 1=breakdown mensual, 0=solo total
   ===================================================================== */
%macro _psi_compute( input_caslib=, train_table=, oot_table=, vars_num=,
    vars_cat=, byvar=, n_buckets=10, mensual=1 );

    %local n v m c z v_aux v_cat es_categorica num_valores lista_var
        lista_var_uni item meses_oot n_meses hay_mensual _psi_old_validvarname;

    %put NOTE: [psi_compute] Iniciando computo PSI...;
    %put NOTE: [psi_compute] vars_num=&vars_num.;
    %put NOTE: [psi_compute] vars_cat=&vars_cat.;
    %put NOTE: [psi_compute] byvar=&byvar. mensual=&mensual.;

    /* ---- 1) Construir lista unificada (categoricas marcadas con #) ----- */
    %let lista_var=&vars_num.;

    %let z=1;
    %let v_cat=%scan(&vars_cat., &z., %str( ));

    %do %while(%length(&v_cat.) > 0);
        %let lista_var=&lista_var. &v_cat.#;
        %let z=%eval(&z. + 1);
        %let v_cat=%scan(&vars_cat., &z., %str( ));
    %end;

    %let lista_var_uni=;
    %let z=1;
    %let item=%scan(&lista_var., &z., %str( ));
    %do %while(%length(&item.) > 0);
        %if %index(%str( )%superq(lista_var_uni)%str( ),
            %str( )%superq(item)%str( ))=0 %then
            %let lista_var_uni=&lista_var_uni. &item.;
        %let z=%eval(&z. + 1);
        %let item=%scan(&lista_var., &z., %str( ));
    %end;
    %let lista_var=&lista_var_uni.;

    /* ---- 2) Copiar tablas CAS a work para procesamiento local ----------
       CAS no soporta INSERT INTO, PROC SORT in-place, PROC TRANSPOSE
       ni PROC DATASETS CHANGE. Se trabaja en work y se copia a casuser
       al final.
       ------------------------------------------------------------------- */
    data _psi_dev;
        set &input_caslib..&train_table.;
    run;

    data _psi_oot;
        set &input_caslib..&oot_table.;
    run;

    /* ---- 3) Obtener lista de meses en OOT ------------------------------ */
    %let meses_oot= ;
    %let n_meses=0;

    %if %length(%superq(byvar)) > 0 %then %do;
        proc sql noprint;
            select distinct &byvar. into :meses_oot separated by ' ' from
                _psi_oot order by &byvar.;

            select count(distinct &byvar.) into :n_meses trimmed from
                _psi_oot;
        quit;
    %end;

    %put NOTE: [psi_compute] Meses OOT: &meses_oot. (n=&n_meses.);

    /* ---- 4) Inicializar CUBO PSI (en work) ----------------------------- */
    %if %length(%superq(byvar)) > 0 %then %do;
        proc sql;
            create table _psi_cubo ( Variable char(64), &byvar. num, PSI
                num format=10.6, Tipo char(15) );
        quit;
    %end;
    %else %do;
        proc sql;
            create table _psi_cubo ( Variable char(64), Periodo num, PSI
                num format=10.6, Tipo char(15) );
        quit;
    %end;

    /* ---- 5) Calcular PSI por variable y mes ---------------------------- */
    %let n=1;
    %let v=%scan(&lista_var., &n., %str( ));

    %do %while(%length(&v.) > 0);

        /* Determinar si es categorica */
        %let es_categorica=0;
        %let v_aux=&v.;

        %if %index(&v., %str(#)) > 0 %then %do;
            %let v_aux=%substr(&v., 1, %eval(%length(&v.) - 1));
            %let es_categorica=1;
        %end;
        %else %do;
            proc sql noprint;
                select count(distinct &v_aux.) into :num_valores trimmed from
                    _psi_oot;
            quit;
            %if &num_valores. <= 10 %then %let es_categorica=1;
        %end;

        %put NOTE: [psi_compute] Variable &v_aux. (categorica=&es_categorica.);

        /*----- PSI Mensual -----*/
        %if &mensual.=1 and &n_meses. > 0 %then %do;
            %let m=1;
            %let c=%scan(&meses_oot., &m., %str( ));

            %do %while(%length(&c.) > 0);
                %_psi_calc( dev=_psi_dev, oot=_psi_oot,
                    oot_where=&byvar.=&c., var=&v_aux.,
                    n_buckets=&n_buckets., flg_continue=%eval(1 -
                    &es_categorica.) );

                proc sql;
                    insert into _psi_cubo (Variable, &byvar., PSI, Tipo)
                        values ("&v_aux.", &c., &psi_valor., "Mensual");
                quit;

                %let m=%eval(&m. + 1);
                %let c=%scan(&meses_oot., &m., %str( ));
            %end;
        %end;

        /*----- PSI Total (DEV vs OOT completo) -----*/
        %_psi_calc( dev=_psi_dev, oot=_psi_oot, var=&v_aux.,
            n_buckets=&n_buckets., flg_continue=%eval(1 - &es_categorica.) );

        %if %length(%superq(byvar)) > 0 %then %do;
            proc sql;
                insert into _psi_cubo (Variable, &byvar., PSI, Tipo) values
                    ("&v_aux.", 999999, &psi_valor., "Total");
            quit;
        %end;
        %else %do;
            proc sql;
                insert into _psi_cubo (Variable, Periodo, PSI, Tipo) values
                    ("&v_aux.", 999999, &psi_valor., "Total");
            quit;
        %end;

        %let n=%eval(&n. + 1);
        %let v=%scan(&lista_var., &n., %str( ));
    %end;

    /* ---- 6) Crear CUBO formato wide (Variable x Mes) ------------------- */
    %if %length(%superq(byvar)) > 0 %then %do;
        proc sql;
            create table _psi_cubo_base as
            select Variable, &byvar., max(PSI) as PSI format=10.6,
                Tipo
            from _psi_cubo
            group by Variable, &byvar., Tipo;
        quit;

        %let hay_mensual=0;

        proc sql noprint;
            select count(*) into :hay_mensual trimmed from _psi_cubo_base where
                Tipo="Mensual";
        quit;

        /* ---- 7) Resumen con estadisticas y alertas --------------------- */
        proc sql;
            create table _psi_resumen as select Variable, max(case when
                Tipo="Total" then PSI else . end) as PSI_Total format=10.6,
                min(case when Tipo="Mensual" then PSI else . end) as PSI_Min
                format=10.6, max(case when Tipo="Mensual" then PSI else . end)
                as PSI_Max format=10.6, mean(case when Tipo="Mensual" then PSI
                else . end) as PSI_Mean format=10.6, std(case when
                Tipo="Mensual" then PSI else . end) as PSI_Std format=10.6,
                sum(case when Tipo="Mensual" and PSI < 0.10 then 1 else 0 end)
                as Meses_Verde, sum(case when Tipo="Mensual" and PSI >= 0.10 and
                PSI < 0.25 then 1 else 0 end) as Meses_Amarillo, sum(case when
                Tipo="Mensual" and PSI >= 0.25 then 1 else 0 end) as Meses_Rojo,
                sum(case when Tipo="Mensual" then 1 else 0 end) as Total_Meses,
                max(case when Tipo="Mensual" then &byvar. else . end) as
                Ultimo_Mes, min(case when Tipo="Mensual" then &byvar. else .
                end) as Primer_Mes from _psi_cubo_base group by Variable;
        quit;

        /* Agregar tendencia y alertas */
        proc sql;
            create table _psi_resumen_tmp as select a.*, b.PSI as
                PSI_Primer_Mes format=10.6, c.PSI as PSI_Ultimo_Mes format=10.6,
                coalesce(c.PSI, 0) - coalesce(b.PSI, 0) as Tendencia
                format=10.6, case when a.PSI_Total < 0.10 then 'VERDE' when
                a.PSI_Total < 0.25 then 'AMARILLO' else 'ROJO' end as
                Semaforo_Total length=10, case when coalesce(c.PSI, 0) -
                coalesce(b.PSI, 0) > 0.05 then 'EMPEORANDO' when coalesce(c.PSI,
                0) - coalesce(b.PSI, 0) < -0.05 then 'MEJORANDO' else 'ESTABLE'
                end as Alerta_Tendencia length=15, case when a.Total_Meses > 0
                then a.Meses_Rojo / a.Total_Meses else 0 end as Pct_Meses_Rojo
                format=percent8.1 from _psi_resumen a left join
                _psi_cubo_base b on a.Variable=b.Variable and a.Primer_Mes=
                b.&byvar. and b.Tipo="Mensual" left join _psi_cubo_base c on
                a.Variable=c.Variable and a.Ultimo_Mes=c.&byvar. and
                c.Tipo="Mensual";
        quit;

        proc datasets lib=work nolist nowarn;
            delete _psi_resumen;
            change _psi_resumen_tmp=_psi_resumen;
        quit;

    %end;
    %else %do;
        /* Sin byvar: solo cubo wide con Total y resumen simplificado */
        proc sql;
            create table _psi_cubo_base as
            select Variable, Periodo, max(PSI) as PSI format=10.6, Tipo
            from _psi_cubo
            group by Variable, Periodo, Tipo;
        quit;

        proc sql;
            create table _psi_resumen as select Variable, PSI as PSI_Total
                format=10.6, case when PSI < 0.10 then 'VERDE' when PSI < 0.25
                then 'AMARILLO' else 'ROJO' end as Semaforo_Total length=10 from
                _psi_cubo_base where Tipo="Total";
        quit;
    %end;

    /* ---- 8) Copiar resultados finales a casuser (CAS) ------------------ */
    data casuser._psi_cubo;
        set _psi_cubo_base;
    run;

    data casuser._psi_resumen;
        set _psi_resumen;
    run;

    %if %length(%superq(byvar)) > 0 %then %do;
        proc cas;
            session conn;
            table.partition /
                table={caslib="casuser", name="_psi_cubo",
                    groupby={"Variable"}, orderby={"&byvar.", "Tipo"}},
                casout={caslib="casuser", name="_psi_cubo", replace=true};
        quit;
    %end;
    %else %do;
        proc cas;
            session conn;
            table.partition /
                table={caslib="casuser", name="_psi_cubo",
                    groupby={"Variable"}, orderby={"Periodo", "Tipo"}},
                casout={caslib="casuser", name="_psi_cubo", replace=true};
        quit;
    %end;

    %if %length(%superq(byvar)) > 0 %then %do;
        %if &hay_mensual. > 0 %then %do;
            proc fedsql sessref=conn;
                create table casuser._psi_cubo_mensual {options replace=true} as
                select Variable, &byvar., PSI
                from casuser._psi_cubo
                where Tipo='Mensual';
            quit;

            proc cas;
                session conn;
                transpose.transpose /
                    table={name="_psi_cubo_mensual", caslib="casuser"},
                    casout={name="_psi_cubo_wide_base", caslib="casuser",
                        replace=true},
                    transpose={"PSI"},
                    id={"&byvar."},
                    groupby={"Variable"};
            quit;

            proc fedsql sessref=conn;
                create table casuser._psi_cubo_wide {options replace=true} as
                select a.*, b.PSI as PSI_Total
                from casuser._psi_cubo_wide_base a
                left join casuser._psi_cubo b
                    on a.Variable=b.Variable and b.Tipo='Total';
            quit;
        %end;
        %else %do;
            proc fedsql sessref=conn;
                create table casuser._psi_cubo_wide {options replace=true} as
                select Variable, PSI as PSI_Total
                from casuser._psi_cubo
                where Tipo='Total';
            quit;
        %end;
    %end;
    %else %do;
        proc fedsql sessref=conn;
            create table casuser._psi_cubo_wide {options replace=true} as
            select Variable, PSI as PSI_Total
            from casuser._psi_cubo
            where Tipo='Total';
        quit;
    %end;

    proc cas;
        session conn;
        table.partition /
            table={caslib="casuser", name="_psi_cubo_wide",
                groupby={}, orderby={"Variable"}},
            casout={caslib="casuser", name="_psi_cubo_wide", replace=true};
        table.partition /
            table={caslib="casuser", name="_psi_resumen",
                groupby={}, orderby={"Variable"}},
            casout={caslib="casuser", name="_psi_resumen", replace=true};
    quit;

    /* ---- 9) Cleanup work ----------------------------------------------- */
    proc datasets lib=work nolist nowarn;
        delete _psi_dev _psi_oot _psi_cubo _psi_cubo_base _psi_resumen;
    quit;

    %put NOTE: [psi_compute] Computo completado. Tablas en casuser:;
    %put NOTE: [psi_compute] casuser._psi_cubo;
    %put NOTE: [psi_compute] casuser._psi_cubo_wide;
    %put NOTE: [psi_compute] casuser._psi_resumen;

%mend _psi_compute;
