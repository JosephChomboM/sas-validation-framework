/* Describe Universo */
%macro __plot_describe_id(data=, param_byvar=,param_id=);
    *1. Calcula el total de registros por variable tiempo (grafico y tabla);
    *2. Calcula el total de registros duplicados respecto a variable tiempo y variable pk (tabla);
    title "Evolutivo Cuentas - &data";

    proc freq data=&data.;
        tables &param_byvar / out=temp_evolut_cuenta;
    run;

    proc sgplot data=temp_evolut_cuenta;
        vbar &param_byvar / response=Count NOOUTLINE
            FILLATTRS=(color=LIGHTSTEELBLUE) barwidth=0.4;
        yaxis label="Cuentas" min=0;
        xaxis label="&param_byvar";
    run;

    /*title "Duplicados - &data";*/
    PROC SQL noprint;
        CREATE TABLE DUP AS SELECT &param_byvar., &param_id., COUNT(*) AS N FROM
            &data. GROUP BY &param_byvar., &param_id. HAVING N>1;
    QUIT;

    /*proc print data=dup;run;*/
    title;

    proc datasets library=work nolist;
        delete temp_evolut_cuenta;
    quit;
%mend;

%macro __bandas_cuentas(datatable=, timeid=, pk=, is_train=);
    %global mean_cuentas std_cuentas;

    *title "Duplicados - &datatable";
    PROC SQL noprint;
        CREATE TABLE DUP AS SELECT &timeid., &pk., COUNT(*) AS N FROM
            &datatable. GROUP BY &timeid., &pk. HAVING N>1;
    QUIT;

    /*proc print data=Dup; run;*/
    *title;
    title "Evolutivo Cuentas - &datatable";

    proc sort data=&datatable. nodupkey out=SinDuplicado;
        by &timeid. &pk.;
    run;

    proc freq data=SinDuplicado;
        tables &timeid. / out=temp_evolut_cuenta;
    run;

    %if &is_train.=1 %then %do;
        proc sql noprint;
            select Mean(Count) as N into:mean_cuentas separated by ' ' from
                temp_evolut_cuenta;
        quit;
        %put el promedio de cuentas &mean_cuentas;

        proc sql noprint;
            select STD(Count) as STD into:std_cuentas separated by ' ' from
                temp_evolut_cuenta;
        quit;
        %put la desviacion estandar es &std_cuentas;
    %end;

    %let inf=%sysevalf(&mean_cuentas - 2 * &std_cuentas);
    %let sup=%sysevalf(&mean_cuentas + 2 * &std_cuentas);
    %let min_val=0;
    %let max_val=%sysevalf(&mean_cuentas + 3 * &std_cuentas);

    proc sgplot data=temp_evolut_cuenta subpixel noautolegend;
        band x=&timeid. lower=&inf upper=&sup / fillattrs=(color=graydd)
            legendlabel="± 2 Desv. Estandar" name="band1";
        series x=&timeid. y=Count / markers lineattrs=(color=black thickness=2)
            legendlabel="Cuentas" name="serie1";
        refline &mean_cuentas. / lineattrs=(color=red pattern=Dash)
            legendlabel="Overall Mean" name="line1";
        yaxis min=0 max=&max_val. label="Promedio de Cuentas";
        xaxis label="&timeid." type=discrete;
        keylegend "serie1" "band1" / location=inside position=bottomright;
    run;

    %if &is_train.=0 %then %do;
        %let mean_cuentas=0;
        %let std_cuentas=0;
    %end;

    proc datasets library=work nolist;
        delete temp_evolut_cuenta dup sinduplicado;
    quit;

%mend;

%macro __evolutivo_suma_monto(datatable=, param_monto=, timeid=);
    *calcula la suma del monto por cada periodo de tiempo;
    proc sql;
        create table resultado_resumen as select &timeid. ,SUM(&param_monto.) as
            Sum_Monto from &datatable. group by &timeid.;
    quit;

    proc sort data=resultado_resumen;
        by &timeid.;
    run;

    title "Suma &param_monto por &timeid";

    proc sgplot data=resultado_resumen;
        vbar &timeid. / response=Sum_Monto barwidth=1;

        xaxis label="&timeid";
        yaxis label="&param_monto";
    run;

    proc print data=resultado_resumen;
    run;
    title;

    proc datasets library=work nolist;
        delete resultado_resumen;
    run;
%mend;

%macro __plot_describe_monto(data=, montoLocal=, byvarlocal=);
    title "Evolutivo Monto - &montoLocal";

    proc means data=&data n mean nonobs;
        var &montoLocal. ;
        class &byvarlocal.;
        output out=temp_evolut_monto n=N mean=Mean;
    run;

    data temp_evolut_monto2;
        set temp_evolut_monto;
        where _TYPE_ ne 0;
    run;

    title Evolutivo &montoLocal.;

    proc sgplot data=temp_evolut_monto2;
        vline &byvarlocal/ response=Mean markers
            markerattrs=(symbol=circlefilled COLOR=black)
            lineattrs=(color=crimson);
        yaxis label="mean  &montoLocal" valuesformat=COMMA16.0 min=0;
    run;
    title;

    proc datasets library=work nolist;
        delete temp_evolut_monto2 temp_evolut_monto;
    quit;

%mend;
%include "&_root_path/Sources/Modulos/m_universe/universe_macro.sas";

/* Universe and Monto Report */
%macro __describe_universe_report(dataset=, data_type=);

    ods graphics on / outputfmt=svg;

    %if &data_type=TRAIN %then %do;
        ods html5
            file="&&path_troncal_&tr/&_img_path/tro_&tr._seg_&seg._DescribeUniverse_1.html";
        ods excel
            file="&&path_troncal_&tr/&_excel_path/tro_&tr._seg_&seg._DescribeUniverse.xlsx"
            options(sheet_name="TRAIN_DescribeUniverso" sheet_interval="none"
            embedded_titles="yes");
        /* Define nombre de imagen único para cada gráfico */
        %__plot_describe_id(data=&dataset, param_byvar=&_var_time,
            param_id=&_idDataset);
        %if %length(&monto.) > 0 %then %do;
            %__plot_describe_monto(data=&dataset, montoLocal=&monto,
                byvarlocal=&_var_time);
        %end;
        %__bandas_cuentas(datatable=&dataset, timeid=&_var_time, pk=&_idDataset,
            is_train=1);
        %if %length(&monto.) > 0 %then %do;
            %__evolutivo_suma_monto(datatable=&dataset, param_monto=&monto,
                timeid=&_var_time);
        %end;
        ods html5 close;

    %end;
    %else %if &data_type=OOT %then %do;

        ods html5
            file="&&path_troncal_&tr/&_img_path/tro_&tr._seg_&seg._DescribeUniverse_2.html";
        ods excel options(sheet_name="OOT_DescribeUniverso" sheet_interval="now"
            embedded_titles="yes");

        %__plot_describe_id(data=&dataset, param_byvar=&_var_time,
            param_id=&_idDataset);
        %if %length(&monto.) > 0 %then %do;
            %__plot_describe_monto(data=&dataset, montoLocal=&monto,
                byvarlocal=&_var_time);
        %end;
        %__bandas_cuentas(datatable=&dataset, timeid=&_var_time, pk=&_idDataset,
            is_train=0);
        %if %length(&monto.) > 0 %then %do;
            %__evolutivo_suma_monto(datatable=&dataset, param_monto=&monto,
                timeid=&_var_time);
        %end;
        ods excel close;
        ods html5 close;
    %end;

    ods graphics off;
%mend;
/*---------------------------------------------------------------------------
Version: 2.0
Desarrollador: Joseph Chombo
Fecha Release: 31/01/2025
-----------------------------------------------------------------------------*/
%include "&_root_path/Sources/Modulos/m_universe/universe_report.sas";

%macro verify_describe(dataset, data_type=);
    /* Universe and Monto verification */
    %if %sysfunc(exist(&dataset)) %then %do;
        %let nobs=%sysfunc(attrn(%sysfunc(open(&dataset)),nobs));
        %if &nobs > 0 %then %do;
            %if %length(&_var_time) > 0 %then %do;
                %if %length(&_idDataset) > 0 %then %do;

                    %if %length(&monto.)=0 %then %do;
                        %put WARNING: (Describe) Faltan parametros opcionales
                            (monto);
                    %end;

                    %__describe_universe_report(dataset=&dataset,
                        data_type=&data_type);
                %end;
                %else %do;
                    %put WARNING: (Describe) Faltan parametros obligatorios
                        (_idDataset);
                %end;
            %end;
            %else %do;
                %put WARNING: (Describe) No existen varible de tiempo;
            %end;
        %end;
        %else %do;
            %put WARNING: (Describe) The dataset no tiene filas;
        %end;

    %end;
    %else %do;
        %put WARNING: (Describe) no existe el dataset;
    %end;
%mend;
