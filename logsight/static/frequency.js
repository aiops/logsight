var frequency_config = {
    type: 'bar',
    data: {
        labels: {{ frequency_labels }},
        datasets: [{
            label: 'Baseline',
            borderColor: 'rgba(75, 192, 192, 1)',
            backgroundColor: 'rgba(75, 192, 192, 0.1)',
            borderWidth: 3,
            data: {{ frequency_baseline }}
        }, {
            label: 'Candidate',
            borderColor: 'rgba(64, 78, 103, 1)',
            backgroundColor: 'rgba(64, 78, 103, 0.2)',
            borderWidth: 3,
            data: {{ frequency_candidate }}
        }]
    },
    options: {
        responsive: true,
        maintainAspectRatio: false,
        barValueSpacing: 0,
        scales: {
            x: {
                grid: {
                  offset: true
                },
                ticks: {
                   maxTicksLimit: 20,
                   callback: function(t) {
                      var maxLabelLength = 10;
                      if (t.length > maxLabelLength) return t.substr(0, maxLabelLength) + '...';
                      else return 'T' + t;
                   }
                }
            }
        },
        tooltips: {
            callbacks: {
                title: function(t, d) {
                    return d.labels[t[0]];
                }
            }
        }
    }
}

