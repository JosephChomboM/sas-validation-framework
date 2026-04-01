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
     casuser._psi_plot_split  conteos TRAIN/OOT por Variable x Periodo

   Parametros:
     input_caslib  = CASLIB de la tabla de entrada unificada
     input_table   = tabla de scope promovida (ej. _scope_input)
     troncal_id    = troncal para resolver ventanas TRAIN/OOT desde cfg_troncales
     vars_num      = lista de variables numericas (separadas por espacio)
     vars_cat      = lista de variables categoricas (separadas por espacio)
     byvar         = variable temporal para breakdown mensual
     n_buckets     = numero de bins para PROC RANK (default 10)
     mensual       = 1=breakdown mensual, 0=solo total
   ===================================================================== */
%macro _psi_compute(input_caslib=, input_table=, troncal_id=, vars_num=,
    vars_cat=, byvar=, n_buckets=10, mensual=1);

    %local n v m c z v_aux v_cat es_categorica num_valores lista_var
        lista_var_uni item meses_oot n_meses hay_mensual _cfg_byvar _train_min
        _train_max _oot_min _oot_max;

    %let _psi_rc=0;

    %put NOTE: [psi_compute] Iniciando computo PSI...;
    %put NOTE: [psi_compute] input=&input_caslib..&input_table.;
    %put NOTE: [psi_compute] vars_num=&vars_num.;
    %put NOTE: [psi_compute] vars_cat=&vars_cat.;
    %put NOTE: [psi_compute] byvar(modulo)=&byvar. mensual=&mensual.;

    /* ---- 0) Resolver byvar/ventanas del split desde cfg_troncales ------- */
    %let _cfg_byvar=;
    %let _train_min=;
    %let _train_max=;
    %let _oot_min=;
    %let _oot_max=;

    proc sql noprint;
        select strip(byvar),
               strip(put(train_min_mes, best.)),
               strip(put(train_max_mes, best.)),
               strip(put(oot_min_mes, best.)),
               strip(put(oot_max_mes, best.))
          into :_cfg_byvar trimmed,
               :_train_min trimmed,
               :_train_max trimmed,
               :_oot_min trimmed,
               :_oot_max trimmed
        from casuser.cfg_troncales
        where troncal_id=&troncal_id.;
    quit;

    %if %length(%superq(_cfg_byvar))=0 or %length(%superq(_train_min))=0 or
        %length(%superq(_train_max))=0 or %length(%superq(_oot_min))=0 or
        %length(%superq(_oot_max))=0 %then %do;
        %put ERROR: [psi_compute] No se pudo resolver cfg_troncales para troncal=&troncal_id..;
        %let _psi_rc=1;
        %return;
    %end;

    %put NOTE: [psi_compute] Split cfg => byvar=&_cfg_byvar. TRAIN=&_train_min.-&_train_max. OOT=&_oot_min.-&_oot_max..;

    /* ---- 1) Construir lista unificada (categoricas marcadas con #) ------ */
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

    /* ---- 2) Stage CAS unificado con split TRAIN/OOT ---------------------- */
    proc cas;
        session conn;
        table.dropTable / caslib='casuser' name='_psi_input' quiet=true;
        table.dropTable / caslib='casuser' name='_psi_input_stage' quiet=true;
        table.dropTable / caslib='casuser' name='_psi_cubo_mensual' quiet=true;
        table.dropTable / caslib='casuser' name='_psi_cubo_wide_base' quiet=true;
        table.dropTable / caslib='casuser' name='_psi_plot_split' quiet=true;
        table.dropTable / caslib='casuser' name='_psi_plot_base' quiet=true;
        table.dropTable / caslib='casuser' name='_psi_var_list' quiet=true;
    quit;

    proc fedsql sessref=conn;
        create table casuser._psi_input {options replace=true} as
        select 'TRAIN' as _psi_split, a.*
        from &input_caslib..&input_table. a
        where a.&_cfg_byvar. >= &_train_min.
          and a.&_cfg_byvar. <= &_train_max.;
    quit;

    proc fedsql sessref=conn;
        create table casuser._psi_input_stage {options replace=true} as
        select 'OOT' as _psi_split, a.*
        from &input_caslib..&input_table. a
        where a.&_cfg_byvar. >= &_oot_min.
          and a.&_cfg_byvar. <= &_oot_max.;
    quit;

    proc cas;
        session conn;
        table.append /
            source={caslib='casuser', name='_psi_input_stage'},
            target={caslib='casuser', name='_psi_input'};
        table.dropTable / caslib='casuser' name='_psi_input_stage' quiet=true;
    quit;

    /* ---- 3) Staging minimo en work para computo iterativo ----------------
       INSERT INTO loops + PROC RANK siguen en work por limitaciones CAS.
       --------------------------------------------------------------------- */
    data _psi_dev;
        set casuser._psi_input(where=(_psi_split='TRAIN'));
    run;

    data _psi_oot;
        set casuser._psi_input(where=(_psi_split='OOT'));
    run;

    /* ---- 4) Obtener lista de meses en OOT (si byvar de modulo existe) --- */
    %let meses_oot=;
    %let n_meses=0;

    %if %length(%superq(byvar)) > 0 %then %do;
        proc sql noprint;
            select distinct &byvar.
              into :meses_oot separated by ' '
            from _psi_oot
            order by &byvar.;

            select count(distinct &byvar.)
              into :n_meses trimmed
            from _psi_oot;
        quit;
    %end;

    %put NOTE: [psi_compute] Meses OOT: &meses_oot. (n=&n_meses.);

    /* ---- 5) Inicializar cubo en work ------------------------------------ */
    %if %length(%superq(byvar)) > 0 %then %do;
        proc sql;
            create table _psi_cubo (
                Variable char(64),
                &byvar. num,
                PSI num format=10.6,
                Tipo char(15)
            );
        quit;
    %end;
    %else %do;
        proc sql;
            create table _psi_cubo (
                Variable char(64),
                Periodo num,
                PSI num format=10.6,
                Tipo char(15)
            );
        quit;
    %end;

    /* ---- 6) Calcular PSI por variable y mes ------------------------------ */
    %let n=1;
    %let v=%scan(&lista_var., &n., %str( ));

    %do %while(%length(&v.) > 0);

        %let es_categorica=0;
        %let v_aux=&v.;

        %if %index(&v., %str(#)) > 0 %then %do;
            %let v_aux=%substr(&v., 1, %eval(%length(&v.) - 1));
            %let es_categorica=1;
        %end;
        %else %do;
            proc sql noprint;
                select count(distinct &v_aux.) into :num_valores trimmed
                from _psi_oot;
            quit;
            %if &num_valores. <= 10 %then %let es_categorica=1;
        %end;

        %put NOTE: [psi_compute] Variable &v_aux. (categorica=&es_categorica.);

        /* PSI mensual */
        %if &mensual.=1 and %length(%superq(byvar)) > 0 and &n_meses. > 0 %then %do;
            %let m=1;
            %let c=%scan(&meses_oot., &m., %str( ));

            %do %while(%length(&c.) > 0);
                %_psi_calc(dev=_psi_dev, oot=_psi_oot,
                    oot_where=&byvar.=&c., var=&v_aux.,
                    n_buckets=&n_buckets., flg_continue=%eval(1-&es_categorica.));

                proc sql;
                    insert into _psi_cubo (Variable, &byvar., PSI, Tipo)
                    values ("&v_aux.", &c., &psi_valor., "Mensual");
                quit;

                %let m=%eval(&m. + 1);
                %let c=%scan(&meses_oot., &m., %str( ));
            %end;
        %end;

        /* PSI total */
        %_psi_calc(dev=_psi_dev, oot=_psi_oot, var=&v_aux.,
            n_buckets=&n_buckets., flg_continue=%eval(1-&es_categorica.));

        %if %length(%superq(byvar)) > 0 %then %do;
            proc sql;
                insert into _psi_cubo (Variable, &byvar., PSI, Tipo)
                values ("&v_aux.", 999999, &psi_valor., "Total");
            quit;
        %end;
        %else %do;
            proc sql;
                insert into _psi_cubo (Variable, Periodo, PSI, Tipo)
                values ("&v_aux.", 999999, &psi_valor., "Total");
            quit;
        %end;

        %let n=%eval(&n. + 1);
        %let v=%scan(&lista_var., &n., %str( ));
    %end;

    /* ---- 7) Armar cubo base y resumen ----------------------------------- */
    %if %length(%superq(byvar)) > 0 %then %do;
        proc sql;
            create table _psi_cubo_base as
            select Variable, &byvar., max(PSI) as PSI format=10.6, Tipo
            from _psi_cubo
            group by Variable, &byvar., Tipo;
        quit;

        %let hay_mensual=0;
        proc sql noprint;
            select count(*) into :hay_mensual trimmed
            from _psi_cubo_base
            where Tipo='Mensual';
        quit;

        proc sql;
            create table _psi_resumen as
            select Variable,
                   max(case when Tipo='Total' then PSI else . end) as PSI_Total format=10.6,
                   min(case when Tipo='Mensual' then PSI else . end) as PSI_Min format=10.6,
                   max(case when Tipo='Mensual' then PSI else . end) as PSI_Max format=10.6,
                   mean(case when Tipo='Mensual' then PSI else . end) as PSI_Mean format=10.6,
                   std(case when Tipo='Mensual' then PSI else . end) as PSI_Std format=10.6,
                   sum(case when Tipo='Mensual' and PSI < 0.10 then 1 else 0 end) as Meses_Verde,
                   sum(case when Tipo='Mensual' and PSI >= 0.10 and PSI < 0.25 then 1 else 0 end) as Meses_Amarillo,
                   sum(case when Tipo='Mensual' and PSI >= 0.25 then 1 else 0 end) as Meses_Rojo,
                   sum(case when Tipo='Mensual' then 1 else 0 end) as Total_Meses,
                   max(case when Tipo='Mensual' then &byvar. else . end) as Ultimo_Mes,
                   min(case when Tipo='Mensual' then &byvar. else . end) as Primer_Mes
            from _psi_cubo_base
            group by Variable;
        quit;

        proc sql;
            create table _psi_resumen_tmp as
            select a.*, b.PSI as PSI_Primer_Mes format=10.6,
                   c.PSI as PSI_Ultimo_Mes format=10.6,
                   coalesce(c.PSI, 0) - coalesce(b.PSI, 0) as Tendencia format=10.6,
                   case when a.PSI_Total < 0.10 then 'VERDE'
                        when a.PSI_Total < 0.25 then 'AMARILLO'
                        else 'ROJO' end as Semaforo_Total length=10,
                   case when coalesce(c.PSI, 0) - coalesce(b.PSI, 0) > 0.05 then 'EMPEORANDO'
                        when coalesce(c.PSI, 0) - coalesce(b.PSI, 0) < -0.05 then 'MEJORANDO'
                        else 'ESTABLE' end as Alerta_Tendencia length=15,
                   case when a.Total_Meses > 0 then a.Meses_Rojo / a.Total_Meses else 0 end
                        as Pct_Meses_Rojo format=percent8.1
            from _psi_resumen a
            left join _psi_cubo_base b
              on a.Variable=b.Variable and a.Primer_Mes=b.&byvar. and b.Tipo='Mensual'
            left join _psi_cubo_base c
              on a.Variable=c.Variable and a.Ultimo_Mes=c.&byvar. and c.Tipo='Mensual';
        quit;

        proc datasets lib=work nolist nowarn;
            delete _psi_resumen;
            change _psi_resumen_tmp=_psi_resumen;
        quit;

    %end;
    %else %do;
        proc sql;
            create table _psi_cubo_base as
            select Variable, Periodo, max(PSI) as PSI format=10.6, Tipo
            from _psi_cubo
            group by Variable, Periodo, Tipo;
        quit;

        proc sql;
            create table _psi_resumen as
            select Variable,
                   PSI as PSI_Total format=10.6,
                   case when PSI < 0.10 then 'VERDE'
                        when PSI < 0.25 then 'AMARILLO'
                        else 'ROJO' end as Semaforo_Total length=10
            from _psi_cubo_base
            where Tipo='Total';
        quit;
    %end;

    /* ---- 8) Publicar resultados finales a CAS ---------------------------- */
    data casuser._psi_cubo;
        set _psi_cubo_base;
    run;

    data casuser._psi_resumen;
        set _psi_resumen;
    run;

    %if %length(%superq(byvar)) > 0 %then %do;
        proc fedsql sessref=conn;
            create table casuser._psi_plot_base {options replace=true} as
            select &_cfg_byvar. as &byvar.,
                   _psi_split as Split,
                   count(*) as N
            from casuser._psi_input
            group by &_cfg_byvar., _psi_split;
        quit;

        proc fedsql sessref=conn;
            create table casuser._psi_var_list {options replace=true} as
            select distinct Variable
            from casuser._psi_cubo;
        quit;

        proc fedsql sessref=conn;
            create table casuser._psi_plot_split {options replace=true} as
            select v.Variable,
                   p.&byvar.,
                   p.Split,
                   p.N
            from casuser._psi_var_list v
            cross join casuser._psi_plot_base p;
        quit;

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
                    table={name='_psi_cubo_mensual', caslib='casuser', groupby={'Variable'}},
                    casout={name='_psi_cubo_wide_base', caslib='casuser', replace=true},
                    transpose={'PSI'},
                    id={"&byvar."};
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

    %if %length(%superq(byvar)) > 0 %then %do;
        proc cas;
            session conn;
            table.partition /
                table={caslib='casuser', name='_psi_cubo',
                    groupby={'Variable'}, orderby={"&byvar.", 'Tipo'}},
                casout={caslib='casuser', name='_psi_cubo', replace=true};
            table.partition /
                table={caslib='casuser', name='_psi_plot_split',
                    groupby={'Variable'}, orderby={"&byvar.", 'Split'}},
                casout={caslib='casuser', name='_psi_plot_split', replace=true};
        quit;
    %end;
    %else %do;
        proc cas;
            session conn;
            table.partition /
                table={caslib='casuser', name='_psi_cubo',
                    groupby={'Variable'}, orderby={'Periodo', 'Tipo'}},
                casout={caslib='casuser', name='_psi_cubo', replace=true};
        quit;
    %end;

    proc cas;
        session conn;
        table.partition /
            table={caslib='casuser', name='_psi_cubo_wide',
                groupby={}, orderby={'Variable'}},
            casout={caslib='casuser', name='_psi_cubo_wide', replace=true};
        table.partition /
            table={caslib='casuser', name='_psi_resumen',
                groupby={}, orderby={'Variable'}},
            casout={caslib='casuser', name='_psi_resumen', replace=true};
    quit;

    /* ---- 9) Cleanup ------------------------------------------------------ */
    proc cas;
        session conn;
        table.dropTable / caslib='casuser' name='_psi_cubo_mensual' quiet=true;
        table.dropTable / caslib='casuser' name='_psi_cubo_wide_base' quiet=true;
        table.dropTable / caslib='casuser' name='_psi_plot_base' quiet=true;
        table.dropTable / caslib='casuser' name='_psi_var_list' quiet=true;
        table.dropTable / caslib='casuser' name='_psi_input_stage' quiet=true;
        table.dropTable / caslib='casuser' name='_psi_input' quiet=true;
    quit;

    proc datasets lib=work nolist nowarn;
        delete _psi_dev _psi_oot _psi_cubo _psi_cubo_base _psi_resumen;
    quit;

    %put NOTE: [psi_compute] Computo completado. Tablas en casuser:;
    %put NOTE: [psi_compute] casuser._psi_cubo;
    %put NOTE: [psi_compute] casuser._psi_cubo_wide;
    %put NOTE: [psi_compute] casuser._psi_resumen;
    %if %length(%superq(byvar)) > 0 %then
        %put NOTE: [psi_compute] casuser._psi_plot_split;

%mend _psi_compute;

