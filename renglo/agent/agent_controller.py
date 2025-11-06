from renglo.agent.agent_core import AgentCore

class AgentController:

    def __init__(self, config=None, tid=None, ip=None):
        """
        Initialize AgentController with configuration.
        
        Args:
            config (dict): Configuration dictionary
            tid: Transaction ID (optional)
            ip: IP address (optional)
        """
        self.config = config or {}
        self.AGK_1 = AgentCore(config=self.config)

    def triage(self,payload,core_name='core_1'):
        
        action = 'triage'

        # The triage exists because there can be many agent cores. 
        # The triage will direct the call to the right agent
        # A core is a specific way to implement an agent. 
        # Different type of cores:
        #   - Multiple LLM calls or a single optimized call.
        #   - One shot or iterative attempts.
        #   - With or without introspection.
        #   - BDI, ReAct, hybrid or any different type of agent. 
        
        # At the moment we only have one kind of agent: core_1
        result ={}
        
        if core_name == 'portfolio_public':
            result = self.AGK_1.run(payload)
        
        if core_name == 'core_1':
            result = self.AGK_1.run(payload) 
          
        if 'success' not in result or not result['success']:
            
            return {'success':False,'action':action,'output':result,'status':400} 
        
        return {'success':True,'action':action,'output':result,'status':200}
        
          
        