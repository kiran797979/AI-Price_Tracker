import plotly.express as px


class PriceChart:
    def create(self, price_history):
        fig = px.line(price_history, x="timestamp", y="price", title=None)
        fig.update_layout(
            xaxis_title=None,
            yaxis_title="Price ($)",
            showlegend=False,
            margin=dict(l=0, r=0, t=0, b=0),
            height=300,
        )
        fig.update_xaxes(tickformat="%Y-%m-%d %H:%M", tickangle=45)
        fig.update_yaxes(tickprefix="$", tickformat=".2f")
        return fig
