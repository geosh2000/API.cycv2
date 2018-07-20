<?php

class venta_help{

    public static function base($class, $inicio, $fin, $prod = false, $pais = null, $mp = true, $outlet = false, $ag = false){

      
        $class->db->select("a.*, canal, gpoCanal, gpoCanalKpi, marca, pais, tipoCanal, c.dep, vacante, puesto, cc, tipo")
            ->select("IF(dtCreated BETWEEN a.Fecha AND ADDDATE(a.Fecha,1), Localizador, null) as NewLoc", FALSE)
            ->select('ml.asesor')
            ->from('t_hoteles_test a')
            ->join("t_masterlocators ml", "a.Localizador = ml.masterlocatorid", "left")          
            ->join("chanGroups b", "a.chanId = b.id", "left")
            ->join("dep_asesores c", "ml.asesor = c.asesor AND a.Fecha = c.Fecha", "left")
            ->join("cc_apoyo d", "ml.asesor = d.asesor AND a.Fecha BETWEEN d.inicio AND d.fin", "left")
            ->where("a.Fecha BETWEEN", "'$inicio' AND '$fin'", FALSE);
    
        if( $mp ){
            $class->db->where( array( 'marca' => 'Marcas Propias' ) );
        }else{
            $class->db->where( array( 'marca' => 'Marcas Terceros' ) )
                    ->where( array( 'gpoCanalKpi !=' => 'Avianca') )
                    ->where( array( 'gpoCanalKpi !=' => 'COOMEVA') );
        }

        if( $pais != null ){ $class->db->where_in('pais', $pais);  }
        if( !$outlet ){ $class->db->where( array( 'gpoCanalKPI !=' => 'Outlet' ) ); }
        if( !$ag ){ $class->db->where( array( 'gpoCanalKPI !=' => 'Agencias' ) ); }
            

        $tableLocs = $class->db->get_compiled_select();

        if( $class->db->query("CREATE TEMPORARY TABLE base $tableLocs") ){
            return $tableLocs;
        }else{
            errResponse('Error al compilar tabla base', REST_Controller::HTTP_BAD_REQUEST, $class, 'error', $class->db->error());
        }
    }

  public static function ventaF($class, $inicio, $fin, $type, $td=false, $skill, $ag = false){

    $params = array(
        '3'     => array( 'skin' => '3', 'skout' => '52', 'skill' => "(3,52)", 'marca' => "'Marcas Terceros'", 'mp' => false ),
        '7'     => array( 'skin' => '7', 'skout' => '7', 'skill' => "(7)", 'marca' => "'Marcas Terceros'", 'mp' => false ),
        '8'     => array( 'skin' => '8', 'skout' => '8', 'skill' => "(8)", 'marca' => "'Marcas Terceros'", 'mp' => false ),
        '9'     => array( 'skin' => '9', 'skout' => '9', 'skill' => "(9)", 'marca' => "'Marcas Terceros'", 'mp' => false ),
        '4'     => array( 'skin' => '4', 'skout' => '4', 'skill' => "(4)", 'marca' => "'Marcas Terceros'", 'mp' => false ),
        '35'    => array( 'skin' => '35', 'skout' => '5', 'skill' => "(35,5,50)", 'marca' => "'Marcas Propias'", 'mp' => true )
    );

    $fecha = "a.Fecha BETWEEN";
    $fechaVar = "'$inicio' AND '$fin'";

    $class->db->query("DROP TEMPORARY TABLE IF EXISTS locsProdF");

    $class->db->select('a.*, tipo')
            ->select("  CASE 
                        WHEN ml.asesor = 0 AND 3 != ".$params[$skill]['skin']." THEN 0
                        WHEN tipoRsva LIKE '%Tag%' THEN 50 
                        WHEN tipoRsva LIKE '%Out' THEN ".$params[$skill]['skout']."
                        WHEN tipoRsva LIKE '%IN' THEN ".$params[$skill]['skin']."
                        WHEN tipoRsva LIKE '%Presencial%' THEN 29 ELSE 0 END as Skill,
                    CASE
                        WHEN ml.asesor = 0 THEN 'Otros'
                        WHEN
                            tipoRsva LIKE '%PDV%'
                        THEN
                            CASE
                                WHEN cc IS NULL THEN 'PDV'
                                WHEN cc IS NOT NULL THEN 'Apoyo'
                            END
                        WHEN tipoRsva LIKE '%Presencial%' THEN 'Presencial'
                        WHEN tipoRsva LIKE '%online%' THEN 'Online'
                        WHEN
                            tipoRsva LIKE '%out%'
                        THEN
                            CASE
                                WHEN tipoRsva LIKE 'out' THEN 'CC'
                                WHEN tipoRsva LIKE '%Tag%' THEN 'CC'
                                ELSE 'Otros'
                            END
                        WHEN
                            tipoRsva LIKE '%IN'
                        THEN
                            CASE
                                WHEN cc IS NULL THEN 'CC'
                                WHEN cc IS NOT NULL THEN 'Apoyo'
                            END
                        ELSE 'CC'
                    END AS Grupo,
                    SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) AS monto,
                    SUM(IF(servicio = 'Hotel',
                        VentaMXN + OtrosIngresosMXN + EgresosMXN,
                        0)) AS monto_hotel,
                    SUM(IF(servicio = 'Tour',
                        VentaMXN + OtrosIngresosMXN + EgresosMXN,
                        0)) AS monto_tour,
                    SUM(IF(servicio = 'Transfer',
                        VentaMXN + OtrosIngresosMXN + EgresosMXN,
                        0)) AS monto_transfer,
                    SUM(IF(servicio = 'Vuelo',
                        VentaMXN + OtrosIngresosMXN + EgresosMXN,
                        0)) AS monto_vuelo,
                    SUM(IF(servicio = 'Seguro',
                        VentaMXN + OtrosIngresosMXN + EgresosMXN,
                        0)) AS monto_seguro,
                    SUM(clientNights) AS RNs,
                    SUM(costo) AS margen,
                    IF(tipoRsva LIKE '%OUT%'
                            AND IF(dtCreated BETWEEN a.Fecha AND ADDDATE(a.Fecha, 1),
                            a.Localizador,
                            NULL) IS NOT NULL
                            AND SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) > 0,
                        a.Localizador,
                        NULL) AS CountLocOut,
                    IF(tipoRsva LIKE '%IN%'
                            AND IF(dtCreated BETWEEN a.Fecha AND ADDDATE(a.Fecha, 1),
                            a.Localizador,
                            NULL) IS NOT NULL
                            AND SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) > 0,
                        a.Localizador,
                        NULL) AS CountLocIn", FALSE)
            ->from("t_hoteles_test a")
            ->join("t_masterlocators ml", "a.Localizador = ml.masterlocatorid", 'left')
            ->join("chanGroups b", "a.chanId = b.id", 'left')
            ->join("dep_asesores dp", "ml.asesor = dp.asesor AND a.Fecha = dp.Fecha", 'left', FALSE)
            ->join("cc_apoyo ap", "ml.asesor = ap.asesor AND a.Fecha BETWEEN ap.inicio AND ap.fin", 'left', FALSE)
            ->join("itemTypes c", "itemType = c.type AND categoryId = c.category", 'left',FALSE)
            ->join("t_margen d", "a.Localizador = d.Localizador AND a.item = d.Item AND a.Fecha = d.Fecha AND a.Hora=d.Hora", 'left', FALSE)
            ->join("config_tipoRsva tp", "IF(dp.dep IS NULL,
                    IF(ml.asesor = - 1, - 1, 0),
                    IF(dp.dep NOT IN (0 , 3, 5, 29, 35, 50, 52),
                        0,
                        IF(dp.dep = 29 AND cc IS NOT NULL,
                            35,
                            dp.dep))) = tp.dep
                    AND IF(ml.tipo IS NULL OR a.tipo = '',
                    0,
                    ml.tipo) = tp.tipo", 'left', FALSE)
            ->where($fecha, $fechaVar, FALSE)
            ->group_by(array('Fecha','Localizador', 'item'));

    if( $params[$skill]['mp'] ){
        $class->db->where( array( 'marca' => 'Marcas Propias', 'pais' => 'MX' ) );
    }else{
        $class->db->where( array( 'marca' => 'Marcas Terceros', 'gpoCanalKpi !=' => 'Avianca') )
                ->where( array( 'gpoCanalKpi !=' => 'Outlet') )
                ->where( array( 'gpoCanalKpi !=' => 'COOMEVA') );
                if( !$ag ){$class->db->where( array( 'gpoCanalKpi !=' => 'Agencias') );}
    }

    $tableLocs = $class->db->get_compiled_select();

    IF($class->db->query("CREATE TEMPORARY TABLE locsProdF $tableLocs")){    
   
    return $tableLocs;
    }else{
    errResponse('Error al compilar información', REST_Controller::HTTP_BAD_REQUEST, $class, 'error', $class->db->error());
    }


  }

  

}

?>


 
