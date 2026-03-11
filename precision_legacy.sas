%macro __precision_modelo(data=, m_target=, m_pd=, m_segmentation=0, m_segm=);
    ods select none;
    proc means data=&data mean stackods; 
        var &m_target &m_pd;
        ods output summary=prec_total;
    run;
    ods select all;
    
    proc print data=prec_total noobs; 
    run;

    %if &m_segmentation %then %do;
        ods select none;
        proc means data=&data mean stackods; 
            var &m_target &m_pd; 
            class &m_segm; 
            ods output summary=means; 
        run;

        proc transpose data=means out=report;
            by &m_segm; 
            id Variable; 
            var mean;
        run;
        ods select all;
        
        proc print data=report noobs; 
        run;
    %end;

    proc datasets nolist;
       delete means report prec_total;
    run;
%mend;

%macro __precision_modelo_ponderado(data=, m_target=, m_pd=, m_monto=, m_segmentation=0, m_segm=);
    ods select none;
    proc means data=&data mean stackods; 
        var &m_target &m_pd; 
        weight &m_monto;
        ods output summary=prec_total_ponderado;
    run;
    ods select all;

    proc print data=prec_total_ponderado noobs label; 
        var Variable Mean;
        label mean='mean_ponderado';	
    run;

    %if &m_segmentation %then %do;
        ods select none;
        proc means data=&data mean stackods; 
            var &m_target &m_pd; 
            weight &m_monto;
            class &m_segm; 	
            ods output summary=means; 	
        run;

        proc transpose data=means out=report;
            by &m_segm; 
            id Variable; 
            var mean;
        run;
        ods select all;
        
        proc print data=report noobs; 
        run;
    %end;

    proc datasets nolist;
        delete means report prec_total_ponderado;
    run;
%mend;
/*---------------------------------------------------------------------------
  Version: 2.0	  
  Desarrollador: Joseph Chombo					
  Fecha Release: 01/09/2025
-----------------------------------------------------------------------------*/

%include "&_root_path/Sources/Modulos/m_precision/precision_modelo_macro.sas";

%macro __precision_report(data=, r_target=, r_pd=, r_monto=, r_segmentation=, r_segm=, r_data_type=);
    /* Inicializar ODS según data_type */
    
    %if &r_data_type = TRAIN %then %do;
        /* Iniciar Excel con hoja para TRAIN */
        ods _all_ close;
		ods graphics on / outputfmt=svg;
        options dev=actximg;
		ods html5 file="&&path_troncal_&tr/&_img_path/tro_&tr._seg_&seg._Precision_1.html";	
        ods excel file="&&path_troncal_&tr/&_excel_path/tro_&tr._seg_&seg._Precision.xlsx";
        ods excel options(sheet_name="TRAIN_Precision" 
                        sheet_interval="none" 
                        embedded_titles="yes");
        ods graphics on / reset=index;
        
        /* Título para el reporte TRAIN */
        %if &r_segmentation = 1 %then %do;
            title "TRAIN: Precision del Modelo con Segmentacion";
            %__precision_modelo(data=&data, m_target=&r_target, m_pd=&r_pd, m_segmentation=&r_segmentation, m_segm=&r_segm);
            title;
            title "Precision Ponderada por Monto - &r_monto";
            %__precision_modelo_ponderado(data=&data, m_target=&r_target, m_pd=&r_pd,m_monto=&r_monto,m_segmentation=&r_segmentation, m_segm=&r_segm);
            title;
        %end;
        %else %do;
            title "TRAIN: Precision del Modelo";
            %__precision_modelo(data=&data, m_target=&r_target, m_pd=&r_pd, m_segmentation=0, m_segm=&r_segm);
            title;
            title "Precision Ponderada por Monto - &r_monto";
            %__precision_modelo_ponderado(data=&data, m_target=&r_target, m_pd=&r_pd,m_monto=&r_monto,m_segmentation=0, m_segm=&r_segm);
            title;
        %end;
		ods html5 close;
    %end;
    %else %if &r_data_type = OOT %then %do;
		ods html5 file="&&path_troncal_&tr/&_img_path/tro_&tr._seg_&seg._Precision_2.html";
        ods excel options(sheet_name="OOT_Precision" 
                        sheet_interval="now" 
                        embedded_titles="yes");
        ods graphics on / reset=index;
        
        /* Título para el reporte OOT */
        %if &r_segmentation = 1 %then %do;
            title "OOT: Precision del Modelo con Segmentacion";
            %__precision_modelo(data=&data, m_target=&r_target, m_pd=&r_pd, m_segmentation=&r_segmentation, m_segm=&r_segm);
            title;   
            title "Precision Ponderada por Monto - &r_monto";
            %__precision_modelo_ponderado(data=&data, m_target=&r_target, m_pd=&r_pd,m_monto=&r_monto,m_segmentation=&r_segmentation, m_segm=&r_segm);
            title;
        %end;
        %else %do;
            title "OOT: Precision del Modelo";
            %__precision_modelo(data=&data, m_target=&r_target, m_pd=&r_pd, m_segmentation=0, m_segm=&r_segm);
            title;
            title "Precision Ponderada por Monto - &r_monto";
            %__precision_modelo_ponderado(data=&data, m_target=&r_target, m_pd=&r_pd,m_monto=&r_monto,m_segmentation=0, m_segm=&r_segm);
            title;
        %end;
        ods excel close;
		ods html5 close;
    %end;

    ods graphics off;
%mend;
/*---------------------------------------------------------------------------
  Version: 1.0	  
  Desarrollador: Joseph Chombo					
  Fecha Release: 03/02/2025
-----------------------------------------------------------------------------*/

%include "&_root_path/Sources/Modulos/m_precision/precision_report.sas";

%macro verify_precision(dataset, data_type=);
    /* Variables locales para verificación */
    %local v_target v_pd v_monto;
    
    /* Asignar valores desde las variables globales */
    %let v_target = &_target;
    %let v_pd = &var_pd;
    %let v_monto = &monto;
    
    %let segmento = 0; 

    %if %symexist(var_segmentadora) %then %do;
        %if %sysevalf(%length(&var_segmentadora) > 0, boolean) %then %do;
            %let segmento = 1;
        %end;
    %end;
    /* Verificar que exista el dataset */
    %if %sysfunc(exist(&dataset)) %then %do;
        /* Se requiere que target, pd y monto estén definidos */
        %if %length(&v_target) = 0 or %length(&v_pd) = 0 or %length(&v_monto) = 0 %then %do;
            %put WARNING: (Precision) Faltan definir target, PD o monto;
            %return;
        %end;
        
        /* Si se indica segmentación */
        %if &segmento = 1 %then %do;
            %if %length(&var_segmentadora) = 0 %then %do;
                %put WARNING: (Precision) Se requiere definir la variable de segmentacion cuando segmento está activado;
                %return;
            %end;
            %put NOTE: (Precision): Ejecutando análisis con segmentación para &data_type;
            /* Ejecutar precisión con segmentación */
            %__precision_report(
                data=&dataset, 
                r_target=&v_target,
                r_pd=&v_pd,
                r_monto=&v_monto,
                r_segmentation=1, 
                r_segm=&var_segmentadora,
                r_data_type=&data_type
            );
        %end;
        /* Si no se segmenta se ejecuta con segmento = 0 */
        %else %do;
            %put NOTE: (Precision): Ejecutando análisis sin segmentación para &data_type;
            %__precision_report(
                data=&dataset, 
                r_target=&v_target, 
                r_pd=&v_pd,
                r_monto=&v_monto,
                r_segmentation=0,
                r_data_type=&data_type
            );
        %end;
    %end;
    %else %do;
        %put WARNING: (Precision) El dataset &dataset no existe;
    %end;    
%mend;