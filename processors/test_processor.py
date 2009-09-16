""" Test processing class """
class test_processor:
    
    def process(self, message):
        
        print 'In test_processor.test: %s' % message.body
        return True