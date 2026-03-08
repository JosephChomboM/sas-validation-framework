%macro __stability_variables(table,param_time=, lista_var=,lista_var_cat=);

    /* Inicializar variables locales */
    %local c v z v_cat;

    /* Procesar variables categóricas solo si la lista no está vacía */
    %if %length(&lista_var_cat) > 0 %then %do;
        %let z=1;
        %let v_cat=%scan(&lista_var_cat, &z," ");
        *unir variables numericas y categoricas;
        %do %while(%length(%trim(&v_cat))>0);
            %let lista_var=&lista_var &v_cat.#;
            %let z=%eval(&z+1);
            %let v_cat=%scan(&lista_var_cat,&z," ");
        %end;
    %end;

    /* Procesar todas las variables */
    %let c=1;
    %let v=%scan(&lista_var, &c, " ");
    %do %while(%length(%trim(&v))>0);
        /* Verificar que no sea un valor especial o inválido */
        %if %length(&v) > 0 and %substr(&v, 1, 1) ne %str(.) %then %do;
            %if %substr(&v, %length(&v),1) eq %str(#) %then %do;
                %let v=%substr(&v, 1, %length(&v)-1);
                %__stability_var_discreto(&table.,&v.,&param_time.);
            %end;
            %else %do;
                %__stability_var_continuo(&table.,&v.,&param_time.);
            %end;
        %end;
        %else %do;
            %if %length(&v) > 0 %then %do;
                %put NOTA: Saltando variable inválida: &v;
            %end;
        %end;
        %let c=%eval(&c+1);
        %let v=%scan(&lista_var, &c, " ");
    %END;

    proc datasets nolist;
        delete t_: ;
    run;
%mend;

%macro __stability_var_continuo(table, var, var_time);

    %local rnd;
    %let rnd=%sysfunc(int(%sysfunc(ranuni(0))*100000));

    PROC SQL;
        CREATE TABLE T_RESULT_&rnd. AS SELECT COUNT(*) AS N_Obs, SUM(case when
            &var. is not null then 1 else 0 end) as N, &var_time. AS &var_time.,
            MEAN(&var.) AS prom, SUM(case when &var. is null then 1 else 0 end)
            as MISSING FROM &table. GROUP BY &var_time.;

    QUIT;

    PROC PRINT DATA=T_RESULT_&rnd. noobs;
        title "Estabilidad de la variable - &var.";
    RUN;

    proc sgplot data=T_RESULT_&rnd. subpixel noautolegend;
        yaxis label="Cantidad de registros (N)" discreteorder=data;
        y2axis label="Promedio &var.";
        vbar &var_time. /response=N nooutline barwidth=0.4
            fillattrs=(color=lightblue);
        vline &var_time. /response=prom markers markerattrs=(symbol=circlefilled
            COLOR=black) y2axis lineattrs=(color=black);
        xaxis label="Variable &var_time" valueattrs=(size=8pt);
    run;
%mend;

%macro __stability_var_discreto(table, var, var_time);

    %local rnd;
    %let rnd=%sysfunc(int(%sysfunc(ranuni(0))*100000));

    DATA T_RESULT_&rnd.;
        SET &table.;
        IF MISSING(&VAR.) THEN &VAR.='MISSING';
    RUN;

    PROC SQL;
        CREATE TABLE T_DIS_&rnd. AS SELECT &var_time. AS &var_Time., &var. AS
            &var., COUNT(*) AS N FROM T_RESULT_&rnd. GROUP BY &var_time. ,
            &var.;
    QUIT;

    PROC SQL;
        CREATE TABLE T_DIS_2_&rnd. AS SELECT *, SUM(N) AS frecuencia_por_codmes
            FROM T_DIS_&rnd. GROUP BY &var_time. ;
    QUIT;

    PROC SQL;
        CREATE TABLE T_FINAL_&rnd. AS SELECT &var_time., &var. ,N, (N *100) /
            Frecuencia_por_codmes AS Porcentaje FROM T_DIS_2_&rnd.;
    QUIT;

    PROC PRINT DATA=T_FINAL_&rnd. noobs;
        title "Estabilidad de la variable - &var.";
    RUN;

    proc sgplot data=T_FINAL_&rnd.;
        vbar &var_time./ response=Porcentaje group=&var. groupdisplay=cluster;
        xaxis display=(nolabel);
        yaxis max=100 label="Porcentaje (%)";
    run;

%mend;
/*---------------------------------------------------------------------------
Version: 2.0
Desarrollador: Joseph Chombo
Fecha Release: 01/09/2025
-----------------------------------------------------------------------------*/
%include
    "&_root_path/Sources/Modulos/m_estabilidad_tiempo/estabilidad_tiempo_macro.sas";

%macro __stability_report(data=, _param_time=, v_num=., v_cat=., data_type=);

    ods graphics on / outputfmt=svg;
    %if &data_type=TRAIN %then %do;
        ods HTML5
            file="&&path_troncal_&tr/&_img_path/tro_&tr._seg_&seg._Estabilidad_tiempo_1.html";
        ods excel
            file="&&path_troncal_&tr/&_excel_path/tro_&tr._seg_&seg._Estabilidad_tiempo.xlsx"
            options(sheet_name="TRAIN_Stability" sheet_interval="none"
            embedded_titles="yes");

        %__stability_variables(table=&data, param_time=&_param_time,
            lista_var=&v_num, lista_var_cat=&v_cat);
        ods html5 close;
    %end;
    %else %if &data_type=OOT %then %do;
        /* Agregar hoja OOT al mismo archivo */
        ods HTML5
            file="&&path_troncal_&tr/&_img_path/tro_&tr._seg_&seg._Estabilidad_tiempo_2.html";
        ods excel options(sheet_name="OOT_Stability" sheet_interval="now"
            embedded_titles="yes");
        %__stability_variables(table=&data, param_time=&_param_time,
            lista_var=&v_num, lista_var_cat=&v_cat);
        ods excel close;
        ods html5 close;
    %end;
    ods graphics off;
%mend;
/*---------------------------------------------------------------------------
Version: 2.0
Desarrollador: Joseph Chombo
Fecha Release: 01/09/2025
-----------------------------------------------------------------------------*/
%include
    "&_root_path/Sources/Modulos/m_estabilidad_tiempo/estabilidad_tiempo_report.sas";

%macro verify_stability(dataset, data_type=);
    %if %sysfunc(exist(&dataset)) %then %do;
        %if %length(&_var_time.)=0 %then %do;
            %put ERROR: (Estabilidad) La variable de tiempo (_var_time) es
                obligatoria y no está definida.;
            %return;
        %end;
        %if %length(&vars_num.) > 0 and %length(&vars_cat.)=0 %then %do;
            %__stability_report(data=&dataset, _param_time=&_var_time.,
                v_num=&vars_num., data_type=&data_type);
        %end;
        %else %if %length(&vars_cat.) > 0 and %length(&vars_num.)=0 %then %do;
            %__stability_report(data=&dataset, _param_time=&_var_time.,
                v_cat=&vars_cat., data_type=&data_type);
        %end;
        %else %if %length(&vars_num.) > 0 and %length(&vars_cat.) > 0 %then %do;
            %__stability_report(data=&dataset, _param_time=&_var_time.,
                v_num=&vars_num., v_cat=&vars_cat., data_type=&data_type);
        %end;
        %else %do;
            %put WARNING: (Estabilidad) No se especifico ninguna lista de
                variables numericas o categoricas;
        %end;
    %end;
    %else %do;
        %put WARNING: (Estabilidad) El dataset no existe;
    %end;
%mend;
