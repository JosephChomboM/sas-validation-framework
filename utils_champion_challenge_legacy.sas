%macro _set_partition(
    train_input=,
    oot_input=,
    target_input=,
    time_input=,
    libname_input=,
    libname_output=,
    seed_input=12345
);
    ods exclude all;
    /* Particion 70/30 train-valid. OOT se mantiene aparte */
    proc partition data=&libname_input..&train_input. partind seed=&seed_input. samppct=70;
        by &time_input. &target_input.;
        output out=&libname_input..train_part;
    run;
    data &libname_output..train_part(copies=0 promote=yes); set &libname_input..train_part; run;
    data &libname_output..train; set &libname_input..train_part(where=(_partind_=1)); run;
    data &libname_output..valid; set &libname_input..train_part(where=(_partind_=0)); run;
    data &libname_output..testoot; set &libname_input..&oot_input.; _partind_=2; run;
    data &libname_output..full_data(copies=0 promote=yes); set &libname_output..train &libname_output..valid &libname_output..testoot; run;
    /*data full; set &libname_input..full; run;*/
    ods exclude none;
%mend _set_partition;
/*---------------------------------------------------------------------------
  Version: 2.0	  
  Desarrollador: Joseph Chombo					
  Fecha Release: 09/09/2025
-----------------------------------------------------------------------------*/
%macro _calculate_gini(
    caslib_name=,
    train_input =,
    oot_input   =,
    target_input=,
    xb_pd_input =,
    ml_algo   =
);
    /* Calcular el gini original */
    proc sql;
        create table gini_original(
            Dataset char(20),
            gini_train num format=best32.,
            gini_oot num format=best32.,
            gini_penalizado num format=best32.
        );
    quit;
    %_gini(&caslib_name., &train_input., &target_input., &xb_pd_input., g_orig_tr);
    %_gini(&caslib_name., &oot_input., &target_input., &xb_pd_input., g_orig_oot);
    %let lambda = 0.5;
    %let g_penal = %sysevalf(&g_orig_oot. - %sysevalf(&lambda. * %sysevalf(&g_orig_tr. - &g_orig_oot.)));
    proc sql;
        insert into gini_original values(
            "Tabla Original",
            &g_orig_tr,
            &g_orig_oot,
            &g_penal    
        );
    quit;
    title "Ginis del Modelo Base";
    proc print data=gini_original noobs; format gini_: percent8.2; run;
    title;

%mend;
%macro _gini(caslib_input, data, target, score_var, outmac);

    proc freqtab data=&caslib_input..&data. noprint missing;
        tables &target. * &score_var.  /  measures;
        output out = gini_freqtab smdcr;
    run;
    
    data _null_;
        set gini_freqtab;
        call symputx("&outmac", _smdcr_, 'G');
    run;
    proc datasets nolist; delete gini_freqtab; run;

%mend;
%include "&_root_path/Sources/Macros/_create_sparse_ticks.sas";

%macro _get_gini_mensual(libname_input, tabin, var, target_input, param_report, byvarl = &param_byvar);
    %local rnd;
    %local xticks;
    %local xticks_;
    %let rnd = %sysfunc(int(%sysfunc(ranuni(0))*100000));

    data &libname_input..t_&rnd._0;
        set &libname_input..&tabin;
    run;

    %_create_sparse_ticks(tabla = &libname_input..t_&rnd._0, byvar = &byvarl, byvarvalues = xticks, 
                    byvar_mod = xticks_, mod = 1, nmatch = 2);


    proc freqtab data=&libname_input..t_&rnd._0 noprint missing;
        by &byvarl;
        tables &target_input. * &var / measures;
        output out = gini_&rnd._1 smdcr;
    run;

    /* Obtener valores de Gini de la salida SMDCR */
    data t_&rnd._2(keep=&byvarl gini_index);
        set gini_&rnd._1;
        gini_index = abs(_smdcr_);
    run;

    /* --------- */
    proc sql;
        create table t_&rnd._3 as 
        select 
        a.&byvarl,
        a.gini_index, 
        b.defaults, 
        b.clientes
        from t_&rnd._2 a
        left join 
        (select &byvarl, sum(&target_input.)as defaults, count(*) as clientes from &libname_input..t_&rnd._0 group by &byvarl) b on a.&byvarl=b.&byvarl;
    quit;

    data report_&param_report;
        retain &byvarl clientes defaults gini_index;
        set t_&rnd._3;
    run;

    data report_&param_report(drop=gini_index);
        set report_&param_report;
        gini_&param_report = gini_index;
    run;

    data casuser.report_&param_report(copies=0 promote=yes);
        set report_&param_report;
    run;

    %if %sysfunc(exist(report_&param_report)) %then %do;
        %put SI EXISTE LA TABLA;
    %end;
    %else %do; %put NO EXISTE LA TABLA; %end;
    
%mend;
%macro _sampling_prechallenge(
    libname_input=,
    libname_output=,
    data_input=,
    target_input=,
    time_input=,
    seed=12345
);


       /* Obtener número de filas y columnas */
    %let dsid = %sysfunc(open(&libname_input..&data_input.));
    %let nobs = %sysfunc(attrn(&dsid., NOBS));
    %let nvars = %sysfunc(attrn(&dsid., NVARS));

    * cerrar la tabla;
    %let rc = %sysfunc(close(&dsid.));


    %let total_cells = %sysevalf(&nobs. * &nvars.);
    %put "Total de celdas: " &total_cells.;

    %let sampling_ratio = %sysevalf(%sysevalf(500000 * 50) / &total_cells.);
    %put "Ratio de muestreo: " &sampling_ratio.;
    %let sampling_pct = %sysevalf(&sampling_ratio. * 100);
    
    %if &sampling_ratio. < 1 %then %do;
        %put WARNING DEVELOPER: "Porcentaje de muestreo: " &sampling_pct. "%";
        ods exclude all;
        proc partition data=&libname_input..&data_input. seed=&seed. samppct=&sampling_pct.;
            by &time_input. &target_input.;
            output out=&libname_output..&data_input.;
        run;
        ods exclude none;
    %end;
    %else %do;
        %put WARNING DEVELOPER: "El conjunto de datos es suficientemente pequeño. No se realiza muestreo.";
        %return;
    %end;

%mend;
%include "&_root_path/Sources/Macros/_create_sparse_ticks.sas";

%macro _merge_ginis_mensual(byvarl=, top_models=, app_bhv_flg=, mlmodel=);
    proc sql;
        create table _gini_mensual_bmk_train as
        SELECT  r0.&byvarl,
        r0.clientes,
        r0.defaults,
        r0.gini_0 as gini_orig, 
        r1.gini_1,
        r2.gini_2
        %if &top_models >= 3 %then %do;
            ,r3.gini_3
        %end;
        %if &top_models >= 4 %then %do;
            ,r4.gini_4
        %end;
        %if &top_models >= 5 %then %do;
            ,r5.gini_5
        %end;
        %if &top_models >= 6 %then %do;
            ,r6.gini_6
        %end;
        FROM casuser.report_0 as r0
        INNER JOIN casuser.report_1 as r1 ON r0.&byvarl = r1.&byvarl
        INNER JOIN casuser.report_2 as r2 ON r0.&byvarl = r2.&byvarl
        %if &top_models >= 3 %then %do;
            INNER JOIN casuser.report_3 as r3 ON r0.&byvarl = r3.&byvarl
        %end;
        %if &top_models >= 4 %then %do;
            INNER JOIN casuser.report_4 as r4 ON r0.&byvarl = r4.&byvarl
        %end;
        %if &top_models >= 5 %then %do;
            INNER JOIN casuser.report_5 as r5 ON r0.&byvarl = r5.&byvarl
        %end;
        %if &top_models >= 6 %then %do;
            INNER JOIN casuser.report_6 as r6 ON r0.&byvarl = r6.&byvarl
        %end;
        ;
    quit;
    proc sort data=_gini_mensual_bmk_train;
        by &byvarl;
    run;
    /* Rest of the macro remains unchanged */
    %local xticks;
    %local xticks_;

    %_create_sparse_ticks(tabla = _gini_mensual_bmk_train, byvar = &byvarl, byvarvalues = xticks, 
                        byvar_mod = xticks_, mod = 1, nmatch = 2);
    

    %let thres = 0.4;
    %if &app_bhv_flg = BHV %then %let thres = 0.5;

    title "Gini - Original vs Modelos &mlmodel";

    proc sgplot data=_gini_mensual_bmk_train subpixel;
        band x=&byvarl lower=0 upper=&thres/ fillattrs=(color=LightRed);
        band x=&byvarl lower=&thres upper=%sysevalf(&thres+0.1)/ fillattrs=(color=gold);
        band x=&byvarl lower=%sysevalf(&thres+0.1) upper=1/ fillattrs=(color=LightGreen);
        /* Línea del modelo original en rojo punteado */
        series X=&byvarl Y=gini_orig/ markers markerattrs=(color=red symbol=circlefilled size=8) 
            LINEATTRS=(THICKNESS=2 color=red pattern=2) legendlabel="Modelo Original";
        /* Líneas de los modelos benchmarking */
        series X=&byvarl Y=gini_1/ markers markerattrs=(color=black symbol=circlefilled size=8) 
            LINEATTRS=(THICKNESS=2 color=black) legendlabel="Modelo 1";
        series X=&byvarl Y=gini_2/ markers markerattrs=(color=blue symbol=circlefilled size=8) 
            LINEATTRS=(THICKNESS=2 color=BLUE) legendlabel="Modelo 2";
        %if &top_models. >= 3 %then %do; 
            series X=&byvarl Y=gini_3/ markers markerattrs=(color=purple symbol=circlefilled size=8) 
                LINEATTRS=(THICKNESS=2 color=purple) legendlabel="Modelo 3"; 
        %end;
        %if &top_models. >= 4 %then %do; 
            series X=&byvarl Y=gini_4/ markers markerattrs=(color=brown symbol=circlefilled size=8) 
                LINEATTRS=(THICKNESS=2 color=BROWN) legendlabel="Modelo 4"; 
        %end;
        %if &top_models. >= 5 %then %do; 
            series X=&byvarl Y=gini_5/ markers markerattrs=(color=green symbol=circlefilled size=8) 
                LINEATTRS=(THICKNESS=2 color=green) legendlabel="Modelo 5"; 
        %end;
        xaxis label= "&byvarl" values=(&xticks) valuesdisplay=(&xticks_) valueattrs=(size=8pt) type=discrete;
        yaxis min=0 max=1 valuesformat=percent8. label="Gini";
        keylegend / position=bottom across=3;
    run;
    title;
        
    proc print data=_gini_mensual_bmk_train noobs;
    run;
%mend;
%macro _create_sparse_ticks(tabla, byvar, byvarvalues = xticks, byvar_mod = xticks_, mod=1, nmatch=2);

    proc sql noprint;
        select distinct &byvar into :&byvarvalues separated by " " from &tabla;
    quit;
    
    %let freq = 1;
    %local n v t;
    %let n=1;
    %let v=%scan(&&&byvarvalues, &n," ");

    %do %while(%length(&v)^=0); 
        %if %sysfunc(mod(%sysfunc(substr(&v, %length(&v)-&nmatch+1, &nmatch)), &freq)) ne 0 %then %do;
            %let v_ = %str(" ");
            %if &n =1 %then %do;
                %let t = %sysfunc(catx(%str( ), &v_));
            %end;
            %else %do;
                %let t = %sysfunc(catx(%str( ), &t, &v_));
            %end;
        %end;
        %else %do;
            %if &n =1 %then %do;
                %let t = %sysfunc(catx(%str( ), "&v"));
            %end;
            %else %do;
                %let t = %sysfunc(catx(%str( ), &t, "&v"));
            %end;
        %end;
        %let n=%eval(&n+1);
        %let v=%scan(&&&byvarvalues, &n, " ");
    %end;
    %let &byvar_mod = &t;
%mend _create_sparse_ticks;