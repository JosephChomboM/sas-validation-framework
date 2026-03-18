%macro __segment_champ_m(
    m_session=casauto,
    m_caslib=casuser,
    m_train=,
    m_oot=,
    m_target=,
    m_troncal=,
    m_segmento=,
    m_pd=,
    m_time=,
    m_model_type=,
    output_table=
);
    
    libname metadata "&_root_path./Troncal_&m_troncal./Data";
    
    /* Obtener la lista de tablas que comienzan con metadata_seg_&m_segmento */
    proc sql noprint;
        select memname into :tables separated by ' '
        from dictionary.tables
        where libname = 'METADATA' and 
              memname like "MD_SEG_&m_segmento.%" and
              memtype = 'DATA';
    quit;
    
    /* Verificar si se encontraron tablas */
    %let num_tables = %sysfunc(countw(&tables));
    %if &num_tables = 0 %then %do;
        %put WARNING: No se encontraron tablas que coincidan con el patrón metadata_seg_&m_segmento.;
        %return;
    %end;
    
    /* Nombre de la tabla de salida */
    %if %length(&output_table) = 0 %then %do;
        %let output_table = ml_champion_seg_&m_segmento;
    %end;
    
    /* Crear un data step para hacer append de todas las tablas */
    data work.&output_table.;
        length modelo $20;
        set 
            %do i = 1 %to &num_tables;
                %let table_name = %scan(&tables, &i);
                metadata.&table_name
            %end;
        ;
    run;
    %put NOTE: Se ha creado la tabla combinada work.&output_table con &num_tables tablas combinadas.;

    proc sort data=work.&output_table;
        by descending gini_penalizado;
    run;
    data work.&output_table;
        set work.&output_table;
        cfg_id = _N_;
    run;
    /* Seleccionar la fila con el mejor gini_penalizado (el más alto) */
    data work.&output_table;
        set work.&output_table;
        if _N_ = 1 then output;
    run;
    %put Se ha seleccionado el mejor modelo basado en gini_penalizado.;

    data _null_;
        set work.&output_table;
        call symputx('ml_algo_name', modelo);
        call symputx('var_segmento', var_segmento);
        call symputx('obj_astore', objeto);
    run;

    proc cas;
        session &m_session.;
        libname &m_caslib. cas caslib=&m_caslib.;
        options casdatalimit=ALL;
    quit;
    
    %include "&_root_path/Sources/Macros/_promote_table.sas";

    %_promote_table(
        libname_input=work,
        libname_output=&m_caslib.,
        table_input=&m_train.,
        promote_flag=0
    );
    %_promote_table(
        libname_input=work,
        libname_output=&m_caslib.,
        table_input=&m_oot.,
        promote_flag=0
    );    
    proc sql noprint;
        select distinct &var_segmento. into :segmento_name from &m_caslib..&m_train.;
    quit;
    /* Cargar el modelo campeón */
    proc casutil;
        load casdata="&obj_astore..sashdat" incaslib="&m_caslib." outcaslib="&m_caslib." casout="&obj_astore" replace;
    quit;
    %put El modelo campeón &obj_astore ha sido cargado en &m_caslib.;

    ods exclude all;
    proc astore;
        score data=&m_caslib..&m_train. out=&m_caslib..&m_train._scd_c
        rstore=&m_caslib.."%sysfunc(dequote(&obj_astore))"n copyvar=(&m_target. &m_time.);
    run;
    
    proc astore;
        score data=&m_caslib..&m_oot. out=&m_caslib..&m_oot._scd_c
        rstore=&m_caslib.."%sysfunc(dequote(&obj_astore))"n copyvar=(&m_target. &m_time.);
    run;
    ods exclude none;
    %let pred_var = P_&m_target.1;


    title "Resultados del modelo campeón - Segmento &segmento_name. - Troncal &m_troncal.";
    proc print data=work.&output_table noobs;
        var modelo troncal segmento var_segmento objeto gini_train gini_oot gini_penalizado;
    run;
    title;

    %include "&_root_path/Sources/Macros/_gini_mensual.sas";
    %_gini_mensual(
        caslib_input=&m_caslib.,
        data_input=&m_train._scd_c,
        target_input=&m_target.,
        score_var=&pred_var.,
        time_input=&m_time.,
        output_table=gini_mensual_champion
    );

    %__gini_mensual_champion(
        m_caslib=&m_caslib.,
        m_table=&m_train._scd_c,
        m_time=&m_time.,
        m_segmento_name=&segmento_name.,
        m_model_type=&m_model_type.,
        m_troncal=&m_troncal.,
        m_data_type=TRAIN,
        m_modelo=&ml_algo_name.
    );

    %_gini_mensual(
        caslib_input=&m_caslib.,
        data_input=&m_oot._scd_c,
        target_input=&m_target.,
        score_var=&pred_var.,
        time_input=&m_time.,
        output_table=gini_mensual_champion
    );
    
    %__gini_mensual_champion(
        m_caslib=&m_caslib.,
        m_table=&m_oot._scd_c,
        m_time=&m_time.,
        m_segmento_name=&segmento_name.,
        m_model_type=&m_model_type.,
        m_troncal=&m_troncal.,
        m_data_type=OOT,
        m_modelo=&ml_algo_name.
    );

    proc datasets lib=work nolist;
        delete gini_: ml_:;
    cas &m_session. terminate;
%mend;

%macro __gini_mensual_champion(m_caslib=, m_table=, m_time=, m_segmento_name=, m_model_type=, m_troncal=, m_data_type=, m_modelo=);

    /* Add cuentas por mes */
    proc sql;
        create table gini_mensual_counts as 
        select &m_time., count(*) as N,
               sum(case when &m_target.=1 then 1 else 0 end) as Defaults
        from &m_caslib..&m_table.
        group by &m_time.;
    quit;
    
    /* Join Gini values with counts */
    proc sql;
        create table gini_monthly_combined as
        select a.&m_time., a._smdcr_ as Gini, b.N, b.Defaults
        from gini_mensual_champion a
        left join gini_mensual_counts b
        on a.&m_time. = b.&m_time.;
    quit;

    %let thres = 0.4;
	%if &m_model_type. = BHV %then %let thres = 0.5;

    proc sgplot data=gini_monthly_combined;
        title "Gini &m_modelo. - Segmento &m_segmento_name. - Troncal &m_troncal. - &m_data_type.";
        vbar &m_time. / response=N transparency=0.7 barwidth=0.5 name='bar'
                        fillattrs=(color=gray)
                        legendlabel='Cuentas' DATALABELFITPOLICY=ROTATE;
        vline &m_time. / response=Gini markers 
                        markerattrs=(symbol=circlefilled color=black size=10px)
                        lineattrs=(thickness=0 color=black) 
                        name='line' legendlabel='Gini' y2axis;
        refline &thres. / axis=y2 lineattrs=(color=orange pattern=2 thickness=2) 
                      labelloc=inside labelattrs=(color=orange) name="acep" legendlabel='Aceptable';
        refline %sysevalf(&thres+0.1) / axis=y2 lineattrs=(color=limegreen pattern=2 thickness=2) 
                      labelloc=inside labelattrs=(color=limegreen) name="sat" legendlabel='Satisfactorio';
        yaxis grid display=(nolabel) offsetmin=0;
        yaxis label="Cuentas" min=0;
        y2axis grid label="Gini" min=0 max=1;
        xaxis display=all label="Periodo";
        keylegend 'bar' 'line' 'acep' 'sat'/ position=bottom noborder;
    run;
    title;
    proc print data=gini_monthly_combined noobs; run;
%mend;
%include "&_root_path/Sources/Modulos/m_champion_challenge/__segment_champ_m.sas";

%macro __segment_champ_r(r_train=, r_oot=, r_target=, r_troncal=, r_segmento=, r_pd=, r_time=, r_model_type=);

	ods graphics on / outputfmt=svg;
	ods HTML5 file="&&path_troncal_&tr/&_img_path/tro_&tr._seg_&seg._segment_champion_model.html";
    ods excel file="&&path_troncal_&tr/&_excel_path/tro_&tr._seg_&seg._segment_champion_model.xlsx"
            options(sheet_name="TRAIN OOT Segment Champion Model" 
                    sheet_interval="none" 
                    embedded_titles="yes");
    
    %__segment_champ_m(
        m_train=&r_train.,
        m_oot=&r_oot.,
        m_target=&r_target.,
        m_troncal=&r_troncal.,
        m_segmento=&r_segmento.,
        m_pd=&r_pd.,
        m_time=&r_time.,
        m_model_type=&r_model_type.,
        output_table=
    );
    ods html5 close;
    ods excel close;

%mend;
%include "&_root_path/Sources/Modulos/m_champion_challenge/__segment_champ_r.sas";

%macro __segment_champ_v(v_train, v_oot, v_troncal, v_segmento);

    %let proceed = 1;
    %let train_exists = %sysfunc(exist(&v_train));
    %let oot_exists = %sysfunc(exist(&v_oot));
    
    /* Verificar existencia de datasets */
    %if &train_exists = 0 %then %do;
        %put WARNING DEVELOPER: El dataset de entrenamiento &v_train no existe;
        %let proceed = 0;
    %end;
    %else %do;
        /* Verificar que tenga registros */
        %let train_nobs = %sysfunc(attrn(%sysfunc(open(&v_train)), NOBS));
        %if &train_nobs = 0 %then %do;
            %put WARNING DEVELOPER: El dataset de entrenamiento &v_train existe pero no contiene registros;
            %let proceed = 0;
        %end;
        %else %do;
            %put NOTE: Dataset &v_train validado correctamente con &train_nobs registros;
        %end;
    %end;
    
    %if &oot_exists = 0 %then %do;
        %put WARNING DEVELOPER: El dataset OOT &v_oot no existe;
        %let proceed = 0;
    %end;
    %else %do;
        /* Verificar que tenga registros */
        %let oot_nobs = %sysfunc(attrn(%sysfunc(open(&v_oot)), NOBS));
        %if &oot_nobs = 0 %then %do;
            %put WARNING DEVELOPER: El dataset OOT &v_oot existe pero no contiene registros;
            %let proceed = 0;
        %end;
        %else %do;
            %put NOTE: Dataset &v_oot validado correctamente con &oot_nobs registros;
        %end;
    %end;
    %if %sysevalf(&_target=,boolean) %then %do;
        %put WARNING DEVELOPER: La variable target (&_target) está vacía;
        %let proceed = 0;
    %end;
    %if %sysevalf(&var_pd=,boolean) %then %do;
        %put WARNING DEVELOPER: La variable pd (&var_pd) está vacía;
        %let proceed = 0;
    %end;    
    %if %sysevalf(&_var_time=,boolean) %then %do;
        %put WARNING DEVELOPER: La variable tiempo (&_var_time) está vacía;
        %let proceed = 0;
    %end;
    %if %sysevalf(&v_troncal=,boolean) %then %do;
        %put WARNING DEVELOPER: La variable troncal (&v_troncal) está vacía;
        %let proceed = 0;
    %end;
    %if %sysevalf(&_tipo_modelo=,boolean) %then %do;
        %put WARNING DEVELOPER: La variable tipo de modelo APP o BHV (&_tipo_modelo) está vacía;
        %let proceed = 0;
    %end;    
    /* verificar si la variable segmento es diferente de 0 y esta vacia */
        
    %if %sysevalf(&v_segmento=,boolean) %then %do;
        %put WARNING DEVELOPER: La variable segmento (&v_segmento) está vacía;
        %let proceed = 0;
    %end;
    %else %do;
        %if &v_segmento = 0 %then %do;
            %put WARNING DEVELOPER: El segmento para segment champion debe ser diferente de cero;
            %let proceed = 0;
        %end;
    %end;

    /* Si todas las validaciones pasaron, proceder */
    %if &proceed = 1 %then %do;
        %__segment_champ_r(
            r_train=&v_train.,
            r_oot=&v_oot.,
            r_target=&_target.,
            r_troncal=&v_troncal.,
            r_segmento=&v_segmento.,
            r_pd=&var_pd.,
            r_time=&_var_time.,
            r_model_type=&_tipo_modelo.
        );
    %end;
%mend;