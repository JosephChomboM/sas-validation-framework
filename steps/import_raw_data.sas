/* =========================================================================
   import_raw_data.sas — Pseudo-step (contrato de UI)

   Este archivo NO es ejecutable ni generado por HTML.
   Documenta los IDs de UI (_id_*) que existirían en un .step equivalente
   para la configuración de importación de datos RAW desde ADLS.

   Flujo: ADLS (parquet) → CAS → data/raw/mydataset.sashdat
   Este paso es PREVIO a la preparación de data processed.

   Ref: README.md §5, design.md §5, caslib_lifecycle.md
   ========================================================================= */

/* ---- IDs de UI documentados ------------------------------------------- */
/*                                                                          */
/*  _id_adls_storage       : nombre de la cuenta ADLS                      */
/*                            Tipo: texto ($200)                            */
/*                            Ejemplo: "adlscu1cemmbackp05"                */
/*                            Mapea a: macro var &adls_storage.             */
/*                                                                          */
/*  _id_adls_container     : filesystem/container dentro de ADLS           */
/*                            Tipo: texto ($200)                            */
/*                            Ejemplo: "mi-container"                       */
/*                            Mapea a: macro var &adls_container.           */
/*                                                                          */
/*  _id_adls_parquet_path  : ruta relativa del .parquet dentro del         */
/*                            container ADLS                                */
/*                            Tipo: texto ($500)                            */
/*                            Ejemplo: "data/modelo/dataset_v1.parquet"     */
/*                            Mapea a: macro var &adls_parquet_path.        */
/*                                                                          */
/*  _id_raw_table_name     : nombre del archivo .sashdat de salida         */
/*                            (sin extensión) en data/raw/                  */
/*                            Tipo: texto ($64)                             */
/*                            Default: "mydataset"                          */
/*                            Mapea a: macro var &raw_table.                */
/*                                                                          */
/*  _id_import_enabled     : flag para habilitar/deshabilitar la           */
/*                            importación (si el .sashdat ya existe en raw) */
/*                            Tipo: entero (0=skip, 1=importar)            */
/*                            Default: 1                                    */
/*                            Mapea a: macro var &adls_import_enabled.      */
/*                                                                          */
/* ----------------------------------------------------------------------- */

/* (Opcional) Mapeo referencial a macro variables internas:
   %let adls_storage       = &_id_adls_storage.;
   %let adls_container     = &_id_adls_container.;
   %let adls_parquet_path  = &_id_adls_parquet_path.;
   %let raw_table          = &_id_raw_table_name.;
   %let adls_import_enabled= &_id_import_enabled.;

   La configuración real se define en config.sas o directamente en runner/main.sas.
*/
