$(document).ready(function () {
    const endpoints = {
        summaryStats: '/api/summary_stats',
        hourlyAnalysis: '/api/hourly_analysis',
    };

    function showError(message) {
        console.error('Error:', message);
        const errorHtml = `
            <div class="alert alert-danger alert-dismissible fade show" role="alert">
                ${message}
                <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
            </div>
        `;
        $('#alerts-container').append(errorHtml);
    }

    function validatePlotData(response) {
        if (!response || typeof response !== 'object') {
            console.error("Response is null or not an object:", response);
            return false;
        }

        if (!response.hasOwnProperty('plot')) {
            console.error("'plot' property is missing in response:", response);
            return false;
        }

        if (!Array.isArray(response.plot.data)) {
            console.error("'data' in 'plot' is not an array:", response.plot.data);
            return false;
        }

        if (typeof response.plot.layout !== 'object') {
            console.error("'layout' in 'plot' is not an object:", response.plot.layout);
            return false;
        }

        return true;
    }

    function updateMetrics() {
        console.log('Fetching summary statistics...');
        $.get(endpoints.summaryStats)
            .done(data => {
                console.log('Summary statistics received:', data);

                const avgDistance = parseFloat(data.avg_distance);
                const totalRevenue = parseFloat(data.total_revenue.replace(/[^0-9.-]+/g, ""));

                $('#total-trips').text(data.total_trips);
                $('#total-revenue').text(`$${totalRevenue.toLocaleString()}`);
                $('#avg-distance').text(`${avgDistance.toFixed(2)} miles`);
                

                // Additional insights for summary stats
                $('#metrics-description').html(`
                    <h5>Summary Insights:</h5>
                    <ul>
                        <li>Total Trips: ${data.total_trips}</li>
                        <li>Total Revenue: $${totalRevenue.toLocaleString()}</li>
                        <li>Average Distance per Trip: ${avgDistance.toFixed(2)} miles</li>
                      
                    </ul>
                `);
            })
            .fail((error) => {
                console.error('Failed to fetch summary statistics:', error);
                showError("Failed to load summary statistics");
            });
    }

    // Initialize tooltips
    $('[data-bs-toggle="tooltip"]').tooltip();

    // Load data
    updateMetrics();

});
