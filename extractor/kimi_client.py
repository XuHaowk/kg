"""
Enhanced Moonshot API client with robust error handling and rate limiting
"""
import json
import requests
import time
import random
import logging
import urllib3
from typing import Dict, List, Any, Optional

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from config import KIMI_API_KEY, KIMI_API_ENDPOINT, KIMI_MODEL

class KimiClient:
    """Moonshot API client with enhanced reliability features"""
    
    def __init__(self, api_key: str = KIMI_API_KEY, 
                 api_endpoint: str = KIMI_API_ENDPOINT,
                 model: str = KIMI_MODEL):
        """
        Initialize enhanced client
        
        Args:
            api_key: API key for authentication
            api_endpoint: API endpoint URL
            model: Primary model to use
        """
        # Configure logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger("KimiClient")
        
        # Print diagnostic information
        self.logger.info(f"Initializing with API endpoint: {api_endpoint}")
        self.logger.info(f"Using primary model: {model}")
        self.logger.info(f"API key length: {len(api_key)} characters")
        
        self.api_key = api_key
        self.api_endpoint = api_endpoint
        self.primary_model = model
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }
        
        # Model fallback configuration
        self.available_models = [
            "moonshot-v1-8k",   # Confirmed working - use as primary
            "moonshot-v1-32k",  # Available but may hit rate limits
            "moonshot-v1-128k"  # Available but may hit rate limits
        ]
        
        # Rate limiting configuration
        self.last_request_time = 0
        self.min_request_interval = 1.2  # Slightly more than the 1 second rate limit window
        self.max_retries = 5
        self.backoff_factor = 1.5  # Exponential backoff multiplier
    
    def generate_completion(self, prompt: str, temperature: float = 0.1, 
                         max_tokens: int = 4000, stream: bool = False,
                         system_prompt: Optional[str] = None) -> Dict[str, Any]:
        """
        Generate text completion using Moonshot API with enhanced reliability
        
        Args:
            prompt: Input text prompt
            temperature: Model temperature (0.0 to 1.0)
            max_tokens: Maximum tokens to generate
            stream: Whether to stream the response
            system_prompt: Optional system prompt
            
        Returns:
            API response as dictionary
        """
        # Prepare messages payload
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})
        
        # Try each model in our fallback list until one works
        models_to_try = self.available_models.copy()
        
        # Ensure primary model is tried first if available
        if self.primary_model in models_to_try:
            models_to_try.remove(self.primary_model)
        models_to_try.insert(0, self.primary_model)
        
        # Store the last error for reporting
        last_error = None
        
        # Try each model with retry logic
        for model in models_to_try:
            self.logger.info(f"Attempting to use model: {model}")
            
            # Try with retries for each model
            for attempt in range(1, self.max_retries + 1):
                try:
                    # Respect rate limits with controlled timing
                    self._wait_for_rate_limit()
                    
                    # Prepare payload
                    payload = {
                        "model": model,
                        "messages": messages,
                        "temperature": temperature,
                        "max_tokens": max_tokens,
                        "stream": stream
                    }
                    
                    self.logger.info(f"API request to {model} (attempt {attempt}/{self.max_retries})")
                    
                    # Make the request with proxy bypass and SSL verification disabled
                    response = requests.post(
                        f"{self.api_endpoint}/chat/completions",
                        headers=self.headers,
                        json=payload,
                        timeout=60,
                        verify=False,
                        proxies={"http": None, "https": None}
                    )
                    
                    # Record the request time
                    self.last_request_time = time.time()
                    
                    # Debug response information
                    self.logger.info(f"Response status: {response.status_code}")
                    
                    # Handle different response scenarios
                    if response.status_code == 200:
                        # Success! Return the response
                        self.logger.info(f"Successful response from model {model}")
                        return response.json()
                    
                    elif response.status_code == 429:
                        # Rate limit - extract wait time if available
                        error_data = response.json().get("error", {})
                        error_msg = error_data.get("message", "")
                        self.logger.warning(f"Rate limit exceeded for {model}: {error_msg}")
                        
                        # Extract wait time if provided
                        wait_seconds = 1  # Default
                        import re
                        time_match = re.search(r'after (\d+) seconds', error_msg)
                        if time_match:
                            wait_seconds = int(time_match.group(1))
                        
                        # Wait with a bit of extra buffer
                        wait_time = wait_seconds * 1.2
                        self.logger.info(f"Waiting {wait_time:.2f} seconds before retry")
                        time.sleep(wait_time)
                        continue  # Retry with the same model
                    
                    elif response.status_code == 404:
                        # Model not found - try the next model
                        error_data = response.json().get("error", {})
                        error_msg = error_data.get("message", "")
                        self.logger.warning(f"Model not found: {model} - {error_msg}")
                        break  # Break the retry loop and try next model
                    
                    else:
                        # Other errors - log and continue
                        self.logger.error(f"API error: {response.status_code} - {response.text}")
                        # Apply exponential backoff
                        backoff_time = self.min_request_interval * (self.backoff_factor ** (attempt - 1))
                        time.sleep(backoff_time)
                
                except Exception as e:
                    # Handle connectivity issues
                    self.logger.error(f"Request error: {str(e)}")
                    last_error = e
                    
                    # Apply exponential backoff
                    backoff_time = self.min_request_interval * (self.backoff_factor ** (attempt - 1))
                    self.logger.info(f"Waiting {backoff_time:.2f} seconds before retry")
                    time.sleep(backoff_time)
        
        # If all models and retries failed, return empty response
        self.logger.error("All models failed after multiple attempts")
        return {"choices": [{"message": {"content": ""}}]}
    
    def _wait_for_rate_limit(self):
        """Wait if needed to respect rate limiting"""
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        
        if elapsed < self.min_request_interval:
            wait_time = self.min_request_interval - elapsed
            self.logger.info(f"Rate limit: Waiting {wait_time:.2f} seconds")
            time.sleep(wait_time)

    
    def extract_entities(self, text: str, entity_types: List[str]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Extract entities from text
        
        Args:
            text: Text to extract entities from
            entity_types: List of entity types to extract
            
        Returns:
            Dictionary of entity types to entity lists
        """
        prompt = f"""Extract {', '.join(entity_types)} from the following text:

{text}

Format your response as a JSON with entity types as keys and arrays of entities as values.
Each entity should have a "text" field and an "occurrences" field."""
        
        response = self.generate_completion(
            prompt=prompt,
            temperature=0.1,
            max_tokens=2000
        )
        
        content = response.get("choices", [{}])[0].get("message", {}).get("content", "")
        return self.parse_json_response(content)
    
    def extract_relations(self, text: str, entities: Dict[str, List[Dict[str, Any]]],
                        relation_types: List[str]) -> List[Dict[str, Any]]:
        """
        Extract relations between entities from text
        
        Args:
            text: Text to extract relations from
            entities: Dictionary of entity types to entity lists
            relation_types: List of relation types to extract
            
        Returns:
            List of relations
        """
        # Flatten entities into a single list
        flat_entities = []
        for entity_type, entity_list in entities.items():
            for entity in entity_list:
                flat_entities.append({
                    "text": entity["text"],
                    "type": entity_type
                })
        
        # Create prompt
        entity_str = json.dumps(flat_entities, ensure_ascii=False)
        relation_str = ", ".join(relation_types)
        
        prompt = f"""Extract relations of types [{relation_str}] between the following entities in the text.

Entities: {entity_str}

Text: {text}

Format your response as a JSON array of relations, where each relation has "source", "target", "relation", and "confidence" fields.
The "source" and "target" fields should contain objects with "text" and "type" fields corresponding to the entities."""
        
        response = self.generate_completion(
            prompt=prompt,
            temperature=0.2,
            max_tokens=3000
        )
        
        content = response.get("choices", [{}])[0].get("message", {}).get("content", "")
        json_data = self.parse_json_response(content)
        
        if isinstance(json_data, list):
            return json_data
        
        # If the response is a dictionary with a "relations" key, use that
        if isinstance(json_data, dict) and "relations" in json_data:
            return json_data["relations"]
        
        return []
    
    def parse_json_response(self, response_text: str) -> Dict:
        """
        尝试解析JSON响应文本
        
        Args:
            response_text: 响应文本，可能包含JSON
            
        Returns:
            解析后的JSON字典，如果解析失败则返回空字典
        """
        # 尝试从文本中提取JSON部分
        try:
            # 查找可能的JSON开始和结束位置
            json_start = response_text.find('{')
            json_end = response_text.rfind('}') + 1
            
            if json_start >= 0 and json_end > json_start:
                json_str = response_text[json_start:json_end]
                # Windows JSON解析处理
                return json.loads(json_str)
            
            # 尝试寻找三重反引号包裹的JSON
            json_pattern_start = response_text.find('```json')
            if json_pattern_start >= 0:
                json_content_start = response_text.find('{', json_pattern_start)
                json_pattern_end = response_text.find('```', json_content_start)
                if json_content_start >= 0 and json_pattern_end >= 0:
                    json_str = response_text[json_content_start:json_pattern_end].strip()
                    # Windows JSON解析处理
                    return json.loads(json_str)
            
            # 尝试查找JSON数组
            array_start = response_text.find('[')
            array_end = response_text.rfind(']') + 1
            
            if array_start >= 0 and array_end > array_start:
                json_str = response_text[array_start:array_end]
                # Windows JSON解析处理
                return json.loads(json_str)
            
            # 如果上述方法都失败，则尝试直接解析整个响应
            return json.loads(response_text)
        except json.JSONDecodeError:
            print(f"无法解析JSON响应: {response_text[:100]}...")
            return {}