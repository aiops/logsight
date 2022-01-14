var release_chart_config = {
    type: 'line',
    data: {
        labels: {{ log_level_x_axis }},
        datasets: [
            {% for timeseries in log_level_timeseries %}
            {
                label: '{{ timeseries.0 }}',
                borderColor: '{{ timeseries.1 }}',
                backgroundColor: '{{ timeseries.2 }}',
                data: {{ timeseries.3 }},
                fill: true,
                borderWidth: 3,
                lineTension: 0.4,
                pointBorderWidth: 3,
                pointBorderColor: 'rgba(221,221,226, 0.5)',
                pointBackgroundColor: 'rgba(221,221,226, 0.5)',

            },
            {% endfor %}
        ]
    },
    options: {
        responsive: true,
        spanGaps: false,
        maintainAspectRatio: false,
        plugins: {
            legend: {
                position: 'top',
            },
            title: {
                display: false,
                text: 'Releases'
            },
            annotation: {
                drawTime: 'afterDatasetsDraw',
                annotations: ['7.05', '19.05', '22.05'].map(function(date, index) {
                    return {
                        type: 'line',
                        id: 'vline' + index,
                        mode: 'vertical',
                        value: date,
                        borderColor: 'rgba(0, 0, 0, 0.1)',
                        backgroundColor: 'rgba(250, 0, 0, 0.1)',
                        borderWidth: 4,
                        xMin: date,
                        xMax: date,
                        label: {
                            enabled: true,
                            position: "start",
                            content: ['V3.1', 'V3.2', 'V4.1'][index],
                            borderColor: 'rgba(0, 0, 0, 0.1)',
                            backgroundColor: 'rgba(0, 0, 0, 0.5)'
                        }
                    }
                })
            }
        }
    }
}