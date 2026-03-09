%macro _calcular_cortes(tablain, var, groups);

    %local rnd;
    %let rnd=%sysfunc(int(%sysfunc(ranuni(0))*100000));

    data t_&rnd._1;
        /*set &tablain(keep=&var.);*/
        set &tablain;
        &var=put(&var, F12.4);
    run;

    PROC RANK DATA=t_&rnd._1 out=t_&rnd._2 GROUPS=&groups;
        RANKS RANGO;
        VAR &var;
    RUN;

    PROC SQL;
        CREATE TABLE t_&rnd._3 AS SELECT RANGO, MIN(&var.) AS MINVAL, MAX(&var.)
            AS MAXVAL FROM t_&rnd._2 GROUP BY RANGO;
    QUIT;

    PROC SORT DATA=t_&rnd._3;
        BY RANGO;
    RUN;

    DATA t_&rnd._4;
        SET t_&rnd._3(RENAME=(RANGO=RANGO_INI)) END=EOF;
        RETAIN MARCA 0;
        N=_n_;
        FLAG_INI=0;
        FLAG_FIN=0;
        LAGMAXVAL=LAG(MAXVAL);
        RANGO=RANGO_INI+1;
        IF RANGO_INI=. THEN RANGO=0;
        IF RANGO_INI>=0 THEN MARCA=MARCA+1;
        IF MARCA=1 THEN FLAG_INI=1;
        IF EOF THEN FLAG_FIN=1;
    RUN;

    PROC SQL;
        CREATE TABLE CORTES AS SELECT "&var." AS VARIABLE LENGTH=32, RANGO,
            RANGO_INI, LAGMAXVAL AS INICIO, MAXVAL AS FIN, FLAG_INI, FLAG_FIN,
            CASE WHEN RANGO=0 THEN "00. Missing" WHEN FLAG_INI=1 THEN
            CAT(PUT(RANGO,Z2.),". <-Inf; ", cats(PUT(MAXVAL,F12.4)), "]") WHEN
            FLAG_FIN=1 THEN CAT(PUT(RANGO,Z2.),". <",
            cats(PUT(LAGMAXVAL,F12.4)), "; +Inf>") ELSE CAT(PUT(RANGO,Z2.),
            ". <", cats(PUT(LAGMAXVAL,F12.4)), "; ", cats(PUT(MAXVAL,F12.4)),
            "]") END AS ETIQUETA LENGTH=200 FROM t_&rnd._4;
    QUIT;

    proc datasets nolist;
        delete t_&rnd.:;
    run;

%mend;

%macro __stab_plot_variables(t1, t2,byvar=, lista_var=,lista_var_cat=,
    groups=5);
    %local n v z;
    %let z=1;
    %let v_cat=%scan(&lista_var_cat., &z.," ");
    *unir variables numericas y categoricas;
    %do %while(%length(&v_cat)^=0);
        %let lista_var=&lista_var. &v_cat.#;
        %let z=%eval(&z+1);
        %let v_cat=%scan(&lista_var_cat,&z," ");
    %end;

    %let n=1;
    %let v=%scan(&lista_var, &n," ");
    %do %while(%length(&v)^=0);
        %if %substr(&v, %length(&v),1) eq %str(#) %then %do;
            %let v=%substr(&v, 1, %length(&v)-1);
            %__STABILITY_PLOT(&t1, &t2, &v, n_buckets=&groups, flg_continue=0);
        %end;
        %else %do;
            %__STABILITY_PLOT(&t1, &t2, &v, n_buckets=&groups);
        %end;
        %let n=%eval(&n+1);
        %let v=%scan(&lista_var, &n, " ");

    %end;
%mend;

%macro __stability_plot(base, compare, var, byvarl=&byvar, n_buckets=5,
    flg_continue=1, reuse_cuts=0);

    %local xticks;
    %local xticks_;
    %local rnd;
    %let rnd=%sysfunc(int(%sysfunc(ranuni(0))*100000));
    %lET color_list=mediumblue salmon mediumpurple gold vligb palegreen pab vigb
        lip vpab;
    %_get_mod_list_byvar(tabla=&base, byvar=&byvarl, byvarvalues=xticks,
        byvar_mod=xticks_, mod=1, nmatch=2);

    %local xticks_comp;
    %local xticks_comp_;
    %_get_mod_list_byvar(tabla=&compare, byvar=&byvarl, byvarvalues=xticks_comp,
        byvar_mod=xticks_comp_, mod=1, nmatch=2);

    %if &flg_continue=1 %then %do;

        %if &reuse_cuts=0 %then %do;
            %_calcular_cortes(&base, &var, &groups);

            proc sort data=cortes;
                by rango;
            run;
        %end;

        DATA T_&RND._1;
            SET cortes END=EOF;
            LENGTH QUERY_START 35 QUERY_END 60;
            N=_n_;
            QUERY_START="WHEN ";
            QUERY_END="";
            IF N=1 THEN QUERY_START="CASE WHEN ";
            IF EOF THEN QUERY_END=" END";
        RUN;

        PROC SQL;
            CREATE TABLE T_&RND._2 AS SELECT *, CASE WHEN RANGO=0 THEN
                CAT("IF MISSING(&VAR.)=1 THEN ETIQUETA=",'"',STRIP(ETIQUETA),'";')
                WHEN FLAG_INI=1 THEN
                CAT("IF &VAR.<=",FIN," THEN ETIQUETA=",'"',STRIP(ETIQUETA),'";')
                WHEN FLAG_FIN=1 THEN
                CAT("IF &VAR.>",INICIO," THEN ETIQUETA=",'"',STRIP(ETIQUETA),'";')
                ELSE
                CAT("IF ",INICIO,"<&VAR.<=",FIN," THEN ETIQUETA=",'"',STRIP(ETIQUETA),'";')
                END AS QUERY_BODY FROM T_&RND._1;
        QUIT;

        data myattrmap(keep=id value fillcolor);
            length FILLCOLOR 20;
            SET T_&RND._2;
            ID='MYID';
            VALUE=ETIQUETA;
            FILLCOLOR=scan("&color_list", rango);
        run;

        PROC SQL noprint;
            SELECT QUERY_BODY INTO:DATAAPPLY SEPARATED BY " " FROM T_&RND._2;
        QUIT;

        DATA T_&RND._3;
            SET &base;
            LENGTH ETIQUETA 120;
            *&var = put(&var, comma12.6);
            &DATAAPPLY.;
        RUN;

        DATA T_&RND._4;
            SET &COMPARE;
            LENGTH ETIQUETA 120;
            *&var = put(&var, comma12.6);
            &DATAAPPLY. RUN;

        proc sort data=T_&RND._3;
            by &byvarl;
        run;

        proc freq data=T_&RND._3 noprint;
            BY &byvarl;
            TABLES ETIQUETA/ out=T_&RND._31;
        run;

        title Evolutivo distribucion variable &var. - Dev;

        proc sgplot data=T_&RND._31 dattrmap=myattrmap;
            vbar &byvarl /response=percent group=ETIQUETA groupdisplay=stack
                NOOUTLINE name="bars" attrid=myid barwidth=1;
            keylegend "bars" /title="Rango" opaque;
            xaxis values=(&xticks) valuesdisplay=(&xticks_);
        run;
        title ;

        proc transpose data=T_&RND._31 out=report(drop=_name_ _label_);
            by &byvarl;
            id etiqueta;
            var percent;
        run;

        ods escapechar="^";
        ODS TEXT=" ";
        ODS TEXT=
            "^S={fontweight=bold fontsize=11pt} Bucket percent of &var by &byvarl";
        ODS TEXT=" ";

        proc print data=report noobs;
        run;

        proc sort data=T_&RND._4;
            by &byvarl;
        run;

        proc freq data=T_&RND._4 noprint;
            by &byvarl;
            tables ETIQUETA/ out=T_&RND._41;
        run;

        title Evolutivo distribucion variable &var. - OOT;

        proc sgplot data=T_&RND._41 dattrmap=myattrmap;
            vbar &byvarl /response=percent group=ETIQUETA groupdisplay=stack
                NOOUTLINE name="bars" attrid=myid barwidth=1;
            keylegend "bars" /title="Rango" opaque;
            xaxis values=(&xticks_comp) valuesdisplay=(&xticks_comp_);
        run;
        title ;

        proc transpose data=T_&RND._41 out=report(drop=_name_ _label_);
            by &byvarl;
            id etiqueta;
            var percent;
        run;

        ods escapechar="^";
        ODS TEXT=" ";
        ODS TEXT=
            "^S={fontweight=bold fontsize=11pt} Bucket percent of &var by &byvarl";
        ODS TEXT=" ";

        proc print data=report noobs;
        run;

        proc sql noprint;
            drop table myattrmap;
        quit;

    %end;

    %else %do;

        proc sort data=&base out=base_sorted;
            by &byvarl;
        run;

        proc freq data=base_sorted noprint;
            BY &byvarl;
            TABLES &var/ out=T_&RND._3;
        run;

        proc sql;
            create table prev_fill as select distinct &var as category from
                T_&RND._3 order by &var asc ;
        quit;

        data myattrmap(keep=id value fillcolor);
            length FILLCOLOR 20;
            SET prev_fill;
            ID='MYID';
            VALUE=cats(category);
            FILLCOLOR=scan("&color_list", _N_);
        run;

        data T_&RND._3;
            set T_&RND._3;
            category=cats(&var);
        run;

        title Evolutivo distribucion variable &var. - Dev;

        proc sgplot data=T_&RND._3 DATTRMAP=myattrmap;
            vbar &byvarl /response=percent group=category groupdisplay=stack
                NOOUTLINE name="bars" attrid=MYID barwidth=1;
            xaxis values=(&xticks) valuesdisplay=(&xticks_);
            keylegend "bars" /title="Rango" opaque;
        run;
        title ;

        proc transpose data=t_&rnd._3 out=report(drop=_name_ _label_);
            by &byvarl;
            id &var;
            var percent;
        run;

        ods escapechar="^";
        ODS TEXT=" ";
        ODS TEXT=
            "^S={fontweight=bold fontsize=11pt} Bucket percent of &var by &byvarl";
        ODS TEXT=" ";

        proc print data=report noobs;
        run;

        proc sort data=&compare out=compare_sorted;
            by &byvarl;
            by &byvarl;
        run;

        proc freq data=compare_sorted noprint;
            BY &byvarl;
            TABLES &var/ out=T_&RND._4;
        run;

        data T_&RND._4;
            set T_&RND._4;
            category=cats(&var);
        run;

        title Evolutivo distribucion variable &var. - OOT;

        proc sgplot data=T_&RND._4 DATTRMAP=myattrmap;
            vbar &byvarl /response=percent group=category groupdisplay=stack
                NOOUTLINE name="bars" attrid=MYID barwidth=1;
            xaxis values=(&xticks_comp) valuesdisplay=(&xticks_comp_);
            keylegend "bars" /title="Rango" opaque;
        run;
        title ;

        proc transpose data=T_&RND._4 out=report(drop=_name_ _label_);
            by &byvarl;
            id &var;
            var percent;
        run;

        ods escapechar="^";
        ODS TEXT=" ";
        ODS TEXT=
            "^S={fontweight=bold fontsize=11pt} Bucket percent of &var by &byvarl";
        ODS TEXT=" ";

        proc print data=report noobs;
        run;

        proc sql noprint;
            drop table prev_fill, myattrmap;
        quit;
    %end;

    proc datasets nolist;
        delete cortes report base_sorted compare_sorted t_&rnd.:;
    run;
%mend;

%macro _get_mod_list_byvar(tabla, byvar, byvarvalues=xticks, byvar_mod=xticks_,
    mod=1, nmatch=2);

    proc sql noprint;
        select distinct &byvar into :&byvarvalues separated by " " from &tabla;
    quit;

    /*
    DISEÑO DEL CODIGO - FUNCIONALIDAD NO MAPEADA

    %if &sqlobs >18 %then %let freq=6;
    %else %do;
    %if &sqlobs > 6 %then %let freq=3;
    %else %let freq=1;
    %end;

    %if &mod > 1 %then %let freq = &mod;

     */
    %let freq=1;
    %local n v t;
    %let n=1;
    %let v=%scan(&&&byvarvalues, &n," ");

    %do %while(%length(&v)^=0);
        %if %sysfunc(mod(%sysfunc(substr(&v, %length(&v)-&nmatch+1, &nmatch)),
            &freq)) ne 0 %then %do;
            %let v_=%str(" ");
            %if &n=1 %then %do;
                %let t=%sysfunc(catx(%str( ), &v_));
            %end;
            %else %do;
                %let t=%sysfunc(catx(%str( ), &t, &v_));
            %end;
        %end;
        %else %do;
            %if &n=1 %then %do;
                %let t=%sysfunc(catx(%str( ), "&v"));
            %end;
            %else %do;
                %let t=%sysfunc(catx(%str( ), &t, "&v"));
            %end;
        %end;
        %let n=%eval(&n+1);
        %let v=%scan(&&&byvarvalues, &n, " ");
    %end;
    %let &byvar_mod=&t;
%mend;

/*---------------------------------------------------------------------------
Version: 1.0
Validacion de Similitud entre Muestras
Desarrollador: Joseph Chombo
Fecha: 24/03/2025
-----------------------------------------------------------------------------*/
%macro __similitud_muestras_num(train_data=, oot_data=, vars_num=, target=,
    umbral_verde=10, umbral_amarillo=20);
    /* Crear tabla de resultados */
    data resultados_numericas;
        length Variable 32 Mediana_TRAIN 8 Mediana_OOT 8 MAE 8 RMSE 8
            Diferencia_Pct 8 Similitud 20;
    run;
    /* Combinar variables numericas y target */
    %local todas_vars i var_num total_vars;
    %let todas_vars=&target &vars_num;

    %if %sysevalf(%length(&todas_vars) > 0) %then %do;
        %let total_vars=%sysfunc(countw(&todas_vars, ' '));
        %do i=1 %to &total_vars;
            %let var_num=%scan(&todas_vars, &i);
            %put Procesando variable numerica: &var_num;

            /* Calcular mediana en TRAIN */
            proc means data=&train_data median noprint;
                var &var_num;
                output out=med_train median=mediana;
            run;

            data _null_;
                set med_train;
                call symputx('mediana_train', mediana);
            run;

            proc means data=&oot_data median noprint;
                var &var_num;
                output out=med_oot median=mediana;
            run;

            data _null_;
                set med_oot;
                call symputx('mediana_oot', mediana);
            run;
            /* Calcular metricas de error */
            %local mae rmse diferencia_pct similitud;
            %let mae=%sysfunc(abs(%sysevalf(&mediana_train - &mediana_oot)));
            %let rmse=%sysfunc(sqrt((&mediana_train - &mediana_oot)**2));

            /* Calcular diferencia porcentual evitando division por cero */
            %if %sysevalf(&mediana_train ^= 0) %then %do;
                %let diferencia_pct=%sysevalf(100 * &mae /
                    %sysfunc(abs(&mediana_train)));
            %end;
            %else %if %sysevalf(&mediana_oot=0) %then %do;
                %let diferencia_pct=0; /* Ambos son cero */
            %end;
            %else %do;
                %let diferencia_pct=100; /* Train es 0, OOT no es 0 */
            %end;
            /* Determinar nivel de similitud */
            %if %sysevalf(&diferencia_pct < &umbral_verde) %then %do;
                %let similitud=Alta Similitud;
            %end;
            %else %if %sysevalf(&diferencia_pct < &umbral_amarillo) %then %do;
                %let similitud=Similitud Media;
            %end;
            %else %do;
                %let similitud=Baja Similitud;
            %end;

            data temp;
                length Variable 32 Mediana_TRAIN 8 Mediana_OOT 8 MAE 8 RMSE 8
                    Diferencia_Pct 8 Similitud 20;
                Variable="&var_num";
                Mediana_TRAIN=&mediana_train;
                Mediana_OOT=&mediana_oot;
                MAE=&mae;
                RMSE=&rmse;
                Diferencia_Pct=&diferencia_pct;
                Similitud="&similitud";
            run;

            proc append base=resultados_numericas data=temp;
            run;
        %end;

        proc format;
            value simil_fmt 'Alta Similitud'='LightGreen' 'Similitud Media'=
                'Yellow' 'Baja Similitud'='LightRed';
        run;

        title "Validacion de similitud de muestras TRAIN - OOT";

        proc print data=resultados_numericas label noobs;
            var Variable Mediana_TRAIN Mediana_OOT MAE RMSE Diferencia_Pct;
            var Similitud / style={background=simil_fmt.};
            format Mediana_TRAIN Mediana_OOT 12.4 MAE RMSE 12.4 Diferencia_Pct
                8.1;
            label MAE="Error Abs. Medio" RMSE="Raiz Error Cuad." Diferencia_Pct=
                "Diferencia (%)" Similitud="Nivel de Similitud";
        run;
        title;

        proc datasets lib=work nolist;
            delete med_train med_oot temp resultados_numericas;
        quit;
    %end;
    %else %do;
        %let total_vars=0;
        %put WARNING: No hay variables numericas para analizar.;
    %end;
%mend;

%macro __similitud_muestras_cat(train_data=, oot_data=, vars_cat=,
    umbral_verde=10, umbral_amarillo=20);
    data resultados_categoricas;
        length Variable 32 Moda_TRAIN 100 Moda_OOT 100 Pct_TRAIN 8 Pct_OOT 8
            Diferencia 8 Similitud 20;
    run;
    /* Procesar cada variable categorica en la lista */
    %local i var_cat total_vars;

    %if %sysevalf(%length(&vars_cat) > 0) %then %do;
        %let total_vars=%sysfunc(countw(&vars_cat, ' '));

        %do i=1 %to &total_vars;
            %let var_cat=%scan(&vars_cat, &i);
            %put Procesando variable categorica: &var_cat;

            /* Calcular moda para esta variable en TRAIN */
            proc freq data=&train_data noprint;
                tables &var_cat / out=freq_train missing;
            run;

            data freq_train;
                set freq_train;
                where not missing(&var_cat);
            run;

            proc sort data=freq_train;
                by descending count;
            run;

            data _null_;
                set freq_train(obs=1);
                call symputx('moda_train', &var_cat);
                call symputx('pct_train', percent);
            run;

            /* Calcular moda para esta variable en OOT */
            proc freq data=&oot_data noprint;
                tables &var_cat / out=freq_oot missing;
            run;

            data freq_oot;
                set freq_oot;
                where not missing(&var_cat);
            run;

            proc sort data=freq_oot;
                by descending count;
            run;

            data _null_;
                set freq_oot(obs=1);
                call symputx('moda_oot', &var_cat);
                call symputx('pct_oot', percent);
            run;

            /* Calcular diferencia */
            %local diferencia similitud color;
            %let diferencia=%sysfunc(abs(%sysevalf(&pct_train - &pct_oot)));

            %if %sysevalf(&diferencia < &umbral_verde) %then %do;
                %let similitud=Alta Similitud;
                %let color=LightGreen;
            %end;
            %else %if %sysevalf(&diferencia < &umbral_amarillo) %then %do;
                %let similitud=Similitud Media;
                %let color=Yellow;
            %end;
            %else %do;
                %let similitud=Baja Similitud;
                %let color=LightRed;
            %end;

            data temp;
                length Variable 32 Moda_TRAIN 100 Moda_OOT 100 Pct_TRAIN 8
                    Pct_OOT 8 Diferencia 8 Similitud 20;
                Variable="&var_cat";
                Moda_TRAIN="&moda_train";
                Moda_OOT="&moda_oot";
                Pct_TRAIN=&pct_train;
                Pct_OOT=&pct_oot;
                Diferencia=&diferencia;
                Similitud="&similitud";
            run;

            proc append base=resultados_categoricas data=temp;
            run;
        %end;

        proc format;
            value simil_fmt 'Alta Similitud'='LightGreen' 'Similitud Media'=
                'Yellow' 'Baja Similitud'='LightRed';
        run;

        proc print data=resultados_categoricas label noobs;
            var Variable Moda_TRAIN Pct_TRAIN Moda_OOT Pct_OOT Diferencia;
            var Similitud / style={background=simil_fmt.};
            format Pct_TRAIN Pct_OOT 8.1 Diferencia 8.1;
            label Diferencia="Diferencia (%)" Similitud="Nivel de Similitud";
        run;

        proc datasets lib=work nolist;
            delete freq_train freq_oot temp resultados_numericas
                resultados_categoricas;
        quit;
    %end;
    %else %do;
        %let total_vars=0;
        %put WARNING: No hay variables categoricas para analizar.;
    %end;
%mend;
