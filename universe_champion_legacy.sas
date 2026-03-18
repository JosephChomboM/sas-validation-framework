%macro __universe_champ_m(
    m_session=casauto,
    m_caslib=casuser,
    m_train=,
    m_oot=,
    m_target=,
    m_troncal=,
    m_time=,
    m_model_type=,
    output_table=
);

    libname metadata "&_root_path./Troncal_&m_troncal./Data";

    /* Buscar tablas de segmentos */
    proc sql noprint;
        select memname into :tables separated by ' '
        from dictionary.tables
        where libname = 'METADATA' and 
              memname like "MD_SEG%";
    quit;
    %if %symexist(tables) %then %do;
    %let num_tables = %sysfunc(countw(&tables));
    %end;
    %else %do; %let num_tables=0;%end;
    /* Si no hay segmentos, buscar tablas del universo */
    %if &num_tables = 0 %then %do;
        proc sql noprint;
            select memname into :tables separated by ' '
            from dictionary.tables
            where libname = 'METADATA' and 
                  memname like "MD_UNI%";
        quit;
        %let num_tables = %sysfunc(countw(&tables));
        %let segmentado = 0;
    %end;
    %else %do;
        %let segmentado = 1;
    %end;

    /* Validar que existan tablas */
    %if %sysevalf(%superq(tables)=,boolean) or &num_tables = 0 %then %do;
        %put WARNING: No se encontraron tablas de segmentos ni de universo;
        %return;
    %end;

    /* Nombre de la tabla de salida */
    %if %length(&output_table) = 0 %then %do;
        %let output_table = ml_champion_universe;
    %end;

    /* Crear un data step para hacer append de todas las tablas */
    data work.all_models;
        length modelo $20 objeto $30;
        set 
            %do i = 1 %to &num_tables;
                %let table_name = %scan(&tables, &i);
                metadata.&table_name
            %end;
        ;
    run;
    %put NOTE: Se ha creado la tabla combinada work.all_models con &num_tables tablas combinadas.;

    /* Iniciar sesión CAS si es necesario */
    proc cas;
        session &m_session.;
        libname &m_caslib. cas caslib=&m_caslib.;
        options casdatalimit=ALL;
    quit;
    %include "&_root_path/Sources/Macros/_create_caslib.sas";
    %let chall_caslib = chall;
    %_create_caslib(
        cas_path =&_chall_path.,
        caslib_name =&chall_caslib.,
        lib_caslib =&chall_caslib.,
        global = N,
        cas_sess_name =&m_session.,
        keep_sess = Y
    );
    /* Cargar y promover datos de entrenamiento y oot al caslib */
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

    %if &segmentado = 1 %then %do;

        /* Seleccionar el mejor modelo para cada segmento */
        proc sort data=work.all_models;
            by segmento descending gini_penalizado;
        run;

        data work.&output_table;
            set work.all_models;
            by segmento;
            if first.segmento then output;
        run;

        data work.&output_table;
            set work.&output_table;
            cfg_id = _N_;
        run;

        %put NOTE: Se ha creado la tabla work.&output_table con los mejores modelos para cada segmento;

        title "Mejores modelos por segmento - Troncal &m_troncal";
        proc report data=work.&output_table;
            format segmento modelo var_segmento objeto gini_train gini_oot gini_penalizado;
        run;
        title;

        proc sql noprint;
            select count(*) into :num_segments from work.&output_table;
            select segmento, var_segmento, objeto 
            into :segment_name1-:segment_name%left(&num_segments),
                :segment_var1-:segment_var%left(&num_segments),
                :model_obj1-:model_obj%left(&num_segments)
            from work.&output_table;
        quit;

        %do i = 1 %to &num_segments;
            ods exclude all;
            proc casutil;
                load casdata="&&model_obj&i...sashdat" incaslib="&chall_caslib." 
                    outcaslib="&chall_caslib." casout="&&model_obj&i" replace;
            quit;

            proc astore;
                score data=&m_caslib..&m_train. out=&m_caslib..train_scored_&i
                rstore=&chall_caslib.."%sysfunc(dequote(&&model_obj&i))"n 
                copyvar=(&m_target. &m_time. &&segment_var&i);
            quit;

            proc astore;
                score data=&m_caslib..&m_oot. out=&m_caslib..oot_scored_&i
                rstore=&chall_caslib.."%sysfunc(dequote(&&model_obj&i))"n 
                copyvar=(&m_target. &m_time. &&segment_var&i);
            quit;
            ods exclude none;

            data &m_caslib..train_scored_&i;
                set &m_caslib..train_scored_&i;
                PD_SEG&i = P_&m_target.1;
                drop P_&m_target.0 P_&m_target.1;
            run;

            data &m_caslib..oot_scored_&i;
                set &m_caslib..oot_scored_&i;
                PD_SEG&i = P_&m_target.1;
                drop P_&m_target.0 P_&m_target.1;
            run;
        %end;

        data &m_caslib..train_all_scores;
            merge 
            %do i = 1 %to &num_segments;
                &m_caslib..train_scored_&i
            %end;
            ;
            PD_FINAL = .;
            %do i = 1 %to &num_segments;
                if &&segment_var&i = "&&segment_name&i" then PD_FINAL = PD_SEG&i;
            %end;
        run;

        data &m_caslib..oot_all_scores;
            merge 
            %do i = 1 %to &num_segments;
                &m_caslib..oot_scored_&i
            %end;
            ;
            PD_FINAL = .;
            %do i = 1 %to &num_segments;
                if &&segment_var&i = "&&segment_name&i" then PD_FINAL = PD_SEG&i;
            %end;
        run;

        %let segment_var_report = &segment_var1.;
    %end;
    %else %do;
        proc sort data=work.all_models;
            by descending gini_penalizado;
        run;
        data work.&output_table;
            set work.all_models;
            if _N_ = 1 then output;
            segmento = "UNIVERSE";
            var_segmento = "UNIVERSE";
        run;

        /* Obtener el nombre del modelo campeón */
        %let model_obj = ;
        proc sql noprint;
            select strip(objeto) into :model_obj from work.&output_table;
        quit;
        %let model_obj = %trim(%left(&model_obj));
        ods exclude all;
        proc casutil;
            load casdata="&model_obj..sashdat" incaslib="&chall_caslib." 
                 outcaslib="&chall_caslib." casout="&model_obj" replace;
        quit;

        proc astore;
            score data=&m_caslib..&m_train. out=&m_caslib..train_scored_1
            rstore=&chall_caslib..&model_obj 
            copyvar=(&m_target. &m_time.);
        quit;

        proc astore;
            score data=&m_caslib..&m_oot. out=&m_caslib..oot_scored_1
            rstore=&chall_caslib..&model_obj 
            copyvar=(&m_target. &m_time.);
        quit;
        ods exclude none;
        data &m_caslib..train_all_scores;
            set &m_caslib..train_scored_1;
            PD_FINAL = P_&m_target.1;
            drop P_&m_target.0 P_&m_target.1;
        run;

        data &m_caslib..oot_all_scores;
            set &m_caslib..oot_scored_1;
            PD_FINAL = P_&m_target.1;
            drop P_&m_target.0 P_&m_target.1;
        run;

        %let segment_var_report = UNIVERSE;
    %end;

    /* Calcular el Gini global usando PD_FINAL */
    %include "&_root_path/Sources/Macros/_gini.sas";
    %_gini(&m_caslib., train_all_scores, &m_target., PD_FINAL, g_tr_final);
    %_gini(&m_caslib., oot_all_scores, &m_target., PD_FINAL, g_oot_final);

    %put NOTE: Gini global final para TRAIN: &g_tr_final;
    %put NOTE: Gini global final para OOT: &g_oot_final;

    %let lambda = 0.5;
    %let g_penalized_final = %sysevalf(&g_oot_final. - %sysevalf(&lambda. * %sysevalf(&g_tr_final. - &g_oot_final.)));    

    proc sql;
        create table work.gini_global_summary(
            troncal num,
            var_segmento char(30),
            gini_train num,
            gini_oot num,
            gini_penalizado num
        );
        insert into work.gini_global_summary values(
            &m_troncal.,
            "&segment_var_report.",
            &g_tr_final.,
            &g_oot_final.,
            &g_penalized_final.
        );
    quit;
    title "Gini Global - Troncal &m_troncal.";
    proc print data=work.gini_global_summary noobs;run;
    title;

    %include "&_root_path/Sources/Macros/_gini_mensual.sas";
    %_gini_mensual(
        caslib_input=&m_caslib.,
        data_input=train_all_scores,
        target_input=&m_target.,
        score_var=PD_FINAL,
        time_input=&m_time.,
        output_table=gini_mensual_champion
    );
    %__gini_global_champion(
        m_caslib=&m_caslib.,
        m_table=train_all_scores,
        m_time=&m_time.,
        m_model_type=&m_model_type.,
        m_troncal=&m_troncal.,
        m_data_type=TRAIN
    );    

    %_gini_mensual(
        caslib_input=&m_caslib.,
        data_input=oot_all_scores,
        target_input=&m_target.,
        score_var=PD_FINAL,
        time_input=&m_time.,
        output_table=gini_mensual_champion
    );
    %__gini_global_champion(
        m_caslib=&m_caslib.,
        m_table=oot_all_scores,
        m_time=&m_time.,
        m_model_type=&m_model_type.,
        m_troncal=&m_troncal.,
        m_data_type=OOT
    );  
    cas &m_session. terminate;
    proc datasets library=work nolist;
        delete gini_: all_models ml_:;
    run;    
%mend;

%macro __gini_global_champion(m_caslib=, m_table=, m_time=, m_model_type=, m_troncal=, m_data_type=);

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
        title "Gini Global Challenge - Troncal &m_troncal. - &m_data_type.";
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
    title "Gini Global Challenge - Troncal &m_troncal. - &m_data_type.";
    proc print data=gini_monthly_combined noobs; run;
    title;
%mend;
%include "&_root_path/Sources/Modulos/m_champion_challenge/__universe_champ_m.sas";

%macro __universe_champ_r(r_train=, r_oot=, r_target=, r_troncal=, r_time=, r_model_type=);

	ods graphics on / outputfmt=svg;
	ods HTML5 file="&&path_troncal_&tr/&_img_path/tro_&tr._seg_&seg._universe_champion_model.html";
    ods excel file="&&path_troncal_&tr/&_excel_path/tro_&tr._seg_&seg._universe_champion_model.xlsx"
            options(sheet_name="TRAIN=OOT_UniverseChampModel" 
                    sheet_interval="none" 
                    embedded_titles="yes");
    
    %__universe_champ_m(
        m_train=&r_train.,
        m_oot=&r_oot.,
        m_target=&r_target.,
        m_troncal=&r_troncal.,
        m_time=&r_time.,
        m_model_type=&r_model_type.,
        output_table=
    );
    ods html5 close;
    ods excel close;
%mend;
%include "&_root_path/Sources/Modulos/m_champion_challenge/__universe_champ_r.sas";

%macro __universe_champ_v(v_train, v_oot, v_troncal, v_segmento);

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

    /* Si todas las validaciones pasaron, proceder */
    %if &proceed = 1 %then %do;
        %__universe_champ_r(
            r_train=&v_train.,
            r_oot=&v_oot.,
            r_target=&_target.,
            r_troncal=&v_troncal.,
            r_time=&_var_time.,
            r_model_type=&_tipo_modelo.
        );
    %end;
%mend;
%macro _save_metadata_model(
    tro_var=,
    seg_name=,
    metadata_path=,
    modelabrv=,
    modelo_name=,
    segment_var =
);
    libname metadata "&metadata_path";
    
    %local seg_name_final;
    %if &seg_name. = UNIVERSE %then %do;
        %let seg_name_final=UNIVERSE;
        data metadata.md_&seg_name_final._&modelabrv.(keep=cfg_id troncal segmento modelo var_segmento objeto gini_train gini_oot gini_penalizado);
            set top_5_&modelabrv._models_sort(keep=cfg_id gini_train gini_oot gini_penalizado);
            troncal = &tro_var.;
            segmento = "&seg_name_final";
            modelo = "&modelo_name";
            var_segmento = "UNIVERSE";
            objeto = "&seg_name_final._&modelabrv";
            where cfg_id=1;
        run;

    %end;
    %else %do;
        %let seg_name_final=&seg_name;
        data metadata.md_seg_&seg_name._&modelabrv.(keep=cfg_id troncal segmento modelo var_segmento objeto gini_train gini_oot gini_penalizado);
            set top_5_&modelabrv._models_sort(keep=cfg_id gini_train gini_oot gini_penalizado);
            troncal = &tro_var.;
            segmento = &seg_name.;
            modelo = "&modelo_name";
            var_segmento = "&segment_var";
            objeto = "&seg_name._&modelabrv";
            where cfg_id=1;
        run;        
    %end;
%mend;
