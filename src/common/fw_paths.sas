/* =========================================================================
  fw_paths.sas - Resolver único de rutas processed
   Evita hardcode de paths en módulos y runner.

   Macro pública:
     %fw_path_processed(outvar=, troncal_id=, split=, seg_id=)

   Convención (design.md §3.2):
     - Universo  : troncal_<id>/<split>/base
     - Segmento  : troncal_<id>/<split>/seg<NNN>  (z3. padding)

   Las rutas devueltas son RELATIVAS al CASLIB PROC (PATH-based,
   subdirs=1, mapeado a data/processed/).  NO incluyen extensión
   (.sashdat lo agrega el consumidor: _promote_castable, _load_cas_data, etc.).
   ========================================================================= */

%macro fw_path_processed(outvar=, troncal_id=, split=, seg_id=);

  /* --- Validación mínima ------------------------------------------------ */
  %if %superq(outvar) = %then %do;
    %put ERROR: [fw_path_processed] outvar= es obligatorio.;
    %abort cancel;
  %end;
  %if %superq(troncal_id) = %then %do;
    %put ERROR: [fw_path_processed] troncal_id= es obligatorio.;
    %abort cancel;
  %end;
  %if %upcase(&split.) ne TRAIN and %upcase(&split.) ne OOT %then %do;
    %put ERROR: [fw_path_processed] split= debe ser TRAIN u OOT (recibido: &split.).;
    %abort cancel;
  %end;

  /* --- Construir ruta --------------------------------------------------- */
  %global &outvar.;

  %if %superq(seg_id) = %then %do;
    /* Universo (base) - sin extensión; el consumidor agrega .sashdat */
    %let &outvar. = troncal_&troncal_id./%lowcase(&split.)/base;
  %end;
  %else %do;
    /* Segmento con padding z3. - sin extensión */
    %let _seg_pad = %sysfunc(putn(&seg_id., z3.));
    %let &outvar. = troncal_&troncal_id./%lowcase(&split.)/seg&_seg_pad.;
    %symdel _seg_pad / nowarn;
  %end;

  %put NOTE: [fw_path_processed] &outvar. = &&&outvar.;

%mend fw_path_processed;
