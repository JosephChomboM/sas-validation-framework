/* =========================================================================
   runner/main.sas — Entrypoint único del framework
   Reemplaza .flw/.step.

   Flujo:
     1) Inicialización (sesión CAS, rutas, run_id)
     2) Carga de config (config.sas → casuser.cfg_troncales / cfg_segmentos)
     3) Carga de utilidades comunes (common_public.sas)
     4) Carga de dispatch (run_module.sas, run_method.sas)
     5) Preparación de data processed (fw_prepare_processed)
     6) Ejecución de módulos — orden: segmentos primero, luego base
     7) Cierre

   Requisitos previos:
     - Sesión CAS activa
     - caslib CASUSER accesible
     - Dataset raw cargado en casuser (ej. mydataset)

   Ref: design.md §2.1 capa 5 (Runner), README.md §4
   ========================================================================= */

/* =====================================================================
   1) INICIALIZACIÓN
   ===================================================================== */

/* Raíz del proyecto — ajustar si main.sas se ejecuta desde otro CWD */
%let fw_root = /path/to/framework_validacion;

/* Módulos a ejecutar (space-separated). Extensible: gini psi ... */
%let methods_list = gini psi;

/* Raw table override (default: mydataset) */
%let raw_table = mydataset;

/* ---- Run ID basado en timestamp ------------------------------------ */
data _null_;
  _ts = put(datetime(), E8601DT19.);
  _ts = translate(_ts, "-", ":");
  _ts = compress(_ts, "T");
  call symputx("run_id", cats("run_", _ts));
run;

%put NOTE: ======================================================;
%put NOTE: [main] run_id = &run_id.;
%put NOTE: ======================================================;

/* ---- Crear carpetas de output por run ------------------------------ */
/* En SAS Viya las carpetas físicas dependen del filesystem accesible;  */
/* aquí se crean las rutas lógicas. Ajustar dcreate / systask según OS. */
%macro _create_output_dirs;
  %let _base = &fw_root./outputs/runs/&run_id.;
  %let _dirs = logs reports images tables manifests;
  %let _nd   = %sysfunc(countw(&_dirs., %str( )));

  %do _d = 1 %to &_nd.;
    %let _dir = %scan(&_dirs., &_d., %str( ));
    %let _rc  = %sysfunc(dcreate(&_dir., &_base.));
    %if &_rc. ne %then
      %put NOTE: [main] Creado &_base./&_dir.;
    %else
      %put WARNING: [main] No se pudo crear &_base./&_dir. (puede ya existir).;
  %end;
%mend _create_output_dirs;

/* Crear directorio base del run primero */
data _null_;
  _base = cats("&fw_root./outputs/runs");
  rc1 = dcreate("&run_id.", _base);
run;
%_create_output_dirs;

/* =====================================================================
   2) CARGA DE CONFIGURACIÓN
   ===================================================================== */
%include "&fw_root./config.sas";

/* =====================================================================
   3) CARGA DE UTILIDADES COMUNES
   ===================================================================== */
%include "&fw_root./src/common/common_public.sas";

/* =====================================================================
   4) CARGA DE DISPATCH
   ===================================================================== */
%include "&fw_root./src/dispatch/run_module.sas";
%include "&fw_root./src/dispatch/run_method.sas";

/* =====================================================================
   5) PREPARACIÓN DE DATA PROCESSED
   ===================================================================== */
%fw_prepare_processed(raw_table=&raw_table.);

/* =====================================================================
   6) EJECUCIÓN DE MÓDULOS — SEGMENTOS PRIMERO, LUEGO BASE
   Regla (design.md §6, README.md §4):
     Si la troncal tiene segmentación:
       1. Ejecutar módulos en cada segmento (train y oot).
       2. Ejecutar módulos en el universo/base (train y oot).
   ===================================================================== */

%macro _run_all_methods;

  /* Leer número de troncales */
  proc sql noprint;
    select count(*) into :_n_tr trimmed from casuser.cfg_troncales;
  quit;

  /* Macrovars por troncal */
  data _null_;
    set casuser.cfg_troncales;
    call symputx(cats("_rtr_id_",   _n_), troncal_id);
    call symputx(cats("_rtr_vseg_", _n_), strip(var_seg));
    call symputx(cats("_rtr_nseg_", _n_), n_segments);
  run;

  %do _i = 1 %to &_n_tr.;

    %let _tid  = &&_rtr_id_&_i.;
    %let _vseg = &&_rtr_vseg_&_i.;
    %let _nseg = &&_rtr_nseg_&_i.;

    %put NOTE: ====================================================;
    %put NOTE: [main] Troncal &_tid. (var_seg=&_vseg. n_seg=&_nseg.);
    %put NOTE: ====================================================;

    /* -----------------------------------------------------------
       6a) SEGMENTOS PRIMERO (si hay segmentación)
       ----------------------------------------------------------- */
    %if %superq(_vseg) ne and &_nseg. > 0 %then %do;
      %do _sg = 1 %to &_nseg.;
        %do _sp = 1 %to 2;
          %if &_sp. = 1 %then %let _split = train;
          %else %let _split = oot;

          %run_method(
            method_modules = &methods_list.,
            troncal_id     = &_tid.,
            split          = &_split.,
            seg_id         = &_sg.,
            run_id         = &run_id.
          );
        %end; /* splits */
      %end; /* segmentos */
    %end;

    /* -----------------------------------------------------------
       6b) UNIVERSO / BASE (después de todos los segmentos)
       ----------------------------------------------------------- */
    %do _sp = 1 %to 2;
      %if &_sp. = 1 %then %let _split = train;
      %else %let _split = oot;

      %run_method(
        method_modules = &methods_list.,
        troncal_id     = &_tid.,
        split          = &_split.,
        seg_id         = ,
        run_id         = &run_id.
      );
    %end; /* splits base */

  %end; /* troncales */

  /* Limpieza */
  %do _i = 1 %to &_n_tr.;
    %symdel _rtr_id_&_i. _rtr_vseg_&_i. _rtr_nseg_&_i. / nowarn;
  %end;
  %symdel _n_tr / nowarn;

%mend _run_all_methods;

%_run_all_methods;

/* =====================================================================
   7) CIERRE
   ===================================================================== */
%put NOTE: ======================================================;
%put NOTE: [main] Run &run_id. completado.;
%put NOTE: Outputs en: &fw_root./outputs/runs/&run_id./;
%put NOTE: ======================================================;
