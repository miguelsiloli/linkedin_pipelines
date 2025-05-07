# dashboard/components/layout_components.py
from dash import html, dcc

def create_navbar():
    """Creates the navigation bar component."""
    navbar = html.Nav([
        dcc.Link('Dashboard', href='/', className='nav-link'),
        dcc.Link('Job Listings', href='/jobs', className='nav-link'),
        # Add more links if needed
    ], className='navbar') # Apply CSS class for styling
    return navbar

# You could add other shared components here, like a footer
# def create_footer():
#     return html.Footer(...)