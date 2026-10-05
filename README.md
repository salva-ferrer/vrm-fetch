**Obtiene los datos vitales de generacio y consumo**

Devuelve un objecto json con el formato:

```json
 {
  "timestamp_utc": "2025-09-16T13:16:40.302827Z",
  "timestamp_data": "2025-09-16T12:16:42Z",
  "generación": {
    "solar": {
      "potencia_w": 3142.8909238100614
    },
    "red": {
      "potencia_w": 98.0
    },
    "bateria": {
      "bateria_soc_pct": 84.0
    },
    "alarmas": []
  },
  "consumo": {
    "potencia_w": 2788.5,
    "alarmas": []
  },
  "notes": []
}
```

Timestamp_utc es el tiempo en que se raliza la query.
timestamp_data es el tiempo del dato que se visualiza.

Embos permiten detectar si se esta conectado. timestamp_utc ~ timestamp_data si se esta conectado

El token de la API VRM se lee de `pass show "Victron - VRM/access_token"` (primera línea).
Se puede sobrescribir con la variable `VRM_TOKEN`, o cambiar la entrada de pass con `VRM_PASS_ENTRY`.
