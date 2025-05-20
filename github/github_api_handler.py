import requests
from urllib.parse import urlparse

class GitHubAPIHandler:
    """Handles GitHub API authentication and URL mapping for github.com and GHES, including REST and GraphQL APIs."""
    
    def __init__(self, base_url, token):
        """
        Initialize the GitHub API handler.
        
        Args:
            base_url (str): The base URL of the GitHub instance (e.g., https://github.com or https://ghes.example.com)
            token (str): Personal access token for authentication
        """
        self.base_url = self._normalize_url(base_url)
        self.token = token
        self.api_url = self._determine_api_url()
        self.graphql_url = self._determine_graphql_url()
        self.headers = {
            'Authorization': f'Bearer {self.token}',
            'Accept': 'application/vnd.github.v3+json'
        }
        self.graphql_headers = {
            'Authorization': f'Bearer {self.token}',
            'Accept': 'application/vnd.github+json'
        }
        self.username = self._get_authenticated_user()

    def _normalize_url(self, url):
        """
        Normalize the input URL by removing trailing slashes and ensuring proper format.
        
        Args:
            url (str): The input URL to normalize
            
        Returns:
            str: Normalized URL
        """
        # Ensure URL starts with https:// or http://
        if not url.startswith(('http://', 'https://')):
            url = f'https://{url}'
        
        # Remove trailing slashes
        return url.rstrip('/')

    def _determine_api_url(self):
        """
        Determine the correct API URL based on the base URL.
        
        Returns:
            str: The appropriate API URL
        """
        parsed_url = urlparse(self.base_url)
        hostname = parsed_url.hostname.lower()

        # Check if it's github.com
        if hostname == 'github.com':
            return 'https://api.github.com'
        
        # For GHES, the API endpoint is typically at /api/v3
        return f'{self.base_url}/api/v3'
    
    def _determine_graphql_url(self):
        """
        Determine the correct graphql API URL based on the base URL.
        
        Returns:
            str: The appropriate graphql API URL
        """
        parsed_url = urlparse(self.base_url)
        hostname = parsed_url.hostname.lower()

        # Check if it's github.com
        if hostname == 'github.com':
            return 'https://api.github.com/graphql'
        
        # For GHES, the API endpoint is typically at /api/v3
        return f'{self.base_url}/api/graphql'

    def _get_authenticated_user(self):
        """
        Retrieve the authenticated user's username.
        
        Returns:
            str: The username of the authenticated user
            
        Raises:
            requests.exceptions.RequestException: If the API request fails
            ValueError: If the username cannot be retrieved
        """
        try:
            response = requests.get(
                f'{self.api_url}/user',
                headers=self.headers
            )
            response.raise_for_status()
            user_data = response.json()
            username = user_data.get('login')
            if not username:
                raise ValueError("Unable to retrieve username from API response")
            return username
        except requests.exceptions.RequestException as e:
            raise requests.exceptions.RequestException(f"Failed to fetch user data: {str(e)}")

    def get_username(self):
        """
        Get the authenticated user's username.
        
        Returns:
            str: The username
        """
        return self.username

    def make_api_request(self, endpoint, method='GET', data=None, params=None):
        """
        Make a REST API request to the GitHub API.
        
        Args:
            endpoint (str): The API endpoint (e.g., '/repos/{owner}/{repo}')
            method (str): HTTP method (GET, POST, etc.)
            data (dict): Data to send in the request body
            params (dict): Query parameters
            
        Returns:
            dict: JSON response from the API
            
        Raises:
            requests.exceptions.RequestException: If the API request fails
        """
        url = f'{self.api_url}{endpoint}'
        try:
            response = requests.request(
                method=method,
                url=url,
                headers=self.headers,
                json=data,
                params=params
            )
            response.raise_for_status()
            return response.json() if response.content else {}
        except requests.exceptions.RequestException as e:
            raise requests.exceptions.RequestException(f"REST API request failed: {str(e)}")

    def make_graphql_request(self, query, variables=None):
        """
        Make a GraphQL API request to the GitHub API.
        
        Args:
            query (str): The GraphQL query string
            variables (dict, optional): Variables for the GraphQL query
            
        Returns:
            dict: JSON response from the GraphQL API
            
        Raises:
            requests.exceptions.RequestException: If the GraphQL request fails
            ValueError: If the response contains errors
        """
        payload = {'query': query}
        if variables:
            payload['variables'] = variables

        try:
            response = requests.post(
                self.graphql_url,
                headers=self.graphql_headers,
                json=payload
            )
            response.raise_for_status()
            result = response.json()
            # print(result)
            if 'errors' in result:
                raise ValueError(f"GraphQL query failed: {result['errors']}")
            
            return result.get('data', {})
        except requests.exceptions.RequestException as e:
            raise requests.exceptions.RequestException(f"GraphQL API request failed: {str(e)}")

# Example usage
if __name__ == "__main__":
    try:
        # Example with github.com
        github_handler = GitHubAPIHandler("https://github.com", "your_token_here")
        print(f"GitHub.com API URL: {github_handler.api_url}")
        print(f"Authenticated user: {github_handler.get_username()}")

        # Example REST API request
        repos = github_handler.make_api_request(f"/users/{github_handler.get_username()}/repos")
        print(f"Repositories (REST): {[repo['name'] for repo in repos]}")

        # Example GraphQL request
        graphql_query = """
        query {
            viewer {
                login
                repositories(first: 10) {
                    nodes {
                        name
                    }
                }
            }
        }
        """
        graphql_result = github_handler.make_graphql_request(graphql_query)
        repo_names = [repo['name'] for repo in graphql_result['viewer']['repositories']['nodes']]
        print(f"Repositories (GraphQL): {repo_names}")

        # Example with GHES
        ghes_handler = GitHubAPIHandler("https://ghes.example.com", "your_ghes_token_here")
        print(f"GHES API URL: {ghes_handler.api_url}")
        print(f"Authenticated user: {ghes_handler.get_username()}")

    except Exception as e:
        print(f"Error: {str(e)}")