/* =========================================================================
   fw_paths.sas — Resolver único de rutas processed
   Evita hardcode de paths en módulos y runner.

   Macro pública:
     %fw_path_processed(outvar=, troncal_id=, split=, seg_id=)

   Convención (design.md §3.2):
     - Universo  : troncal_<id>/<split>/base.sashdat
     - Segmento  : troncal_<id>/<split>/seg<NNN>.sashdat  (z3. padding)
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
    /* Universo (base) */
    %let &outvar. = troncal_&troncal_id./%lowcase(&split.)/base.sashdat;
  %end;
  %else %do;
    /* Segmento con padding z3. */
    %let _seg_pad = %sysfunc(putn(&seg_id., z3.));
    %let &outvar. = troncal_&troncal_id./%lowcase(&split.)/seg&_seg_pad..sashdat;
    %symdel _seg_pad / nowarn;
  %end;

  %put NOTE: [fw_path_processed] &outvar. = &&&outvar.;

%mend fw_path_processed;
